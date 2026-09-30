from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_claim_date_quarter_mo = Window.partitionBy("status", F.date_trunc("month", "claim_date_quarter"))
w_policy_id_adjuster_id = Window.partitionBy("policy_id", "adjuster_id")
w_policy_id_by_claim_date = Window.partitionBy("policy_id").orderBy("claim_date", "id")
w_policy_id = Window.partitionBy("policy_id")
w_all_by_reported_at_ties = Window.orderBy("reported_at")
w_policy_id_adjuster_id_by_app = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("approved_amount").desc(), "id")
w_claim_date_month_policy_id_b = Window.partitionBy(F.month("claim_date"), "policy_id").orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_claim_type = Window.partitionBy("status", "claim_type")
w_reported_at_to_date_claim_da = Window.partitionBy(F.to_date("reported_at"), F.date_trunc("month", "claim_date")).orderBy(F.col("distinct_claim_type_in_status_claim_date_quarter").asc_nulls_last(), "id")
w_reported_at_month_by_last_kn = Window.partitionBy(F.month("reported_at")).orderBy("last_known_reported_at", "id")
w_all_by_distinct_claim_type_i = Window.orderBy("distinct_claim_type_in_status_claim_date_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_claims = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.date_trunc("quarter", F.col("claim_date")).alias("claim_date_quarter"),
    )
)

with_windows = (
    cleaned_claims
    .select(
        "*",
        F.size(F.collect_set("claim_type").over(w_status_claim_date_quarter_mo)).alias("distinct_claim_type_in_status_claim_date_quarter"),
        F.round(F.avg("approved_amount").over(w_policy_id_adjuster_id), 2).alias("policy_id_adjuster_id_avg_approved_amount"),
        F.lag("claimed_amount", 2).over(w_policy_id_by_claim_date).alias("prev2_claimed_amount"),
        F.round(F.avg("claimed_amount").over(w_policy_id), 2).alias("policy_id_avg_claimed_amount"),
        F.dense_rank().over(w_all_by_reported_at_ties).alias("reported_at_dense_rank"),
        F.count(F.lit(1)).over(w_policy_id_adjuster_id).alias("rows_in_policy_id_adjuster_id"),
        F.row_number().over(w_policy_id_adjuster_id_by_app).alias("approved_amount_seq"),
        F.size(F.collect_set("claim_type").over(w_policy_id_adjuster_id)).alias("distinct_claim_type_in_policy_id_adjuster_id"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_id_adjuster_id), F.lit(0)), 4).alias("approved_amount_share_of_policy_id_adjuster_id"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_id), F.lit(0)), 4).alias("approved_amount_share_of_policy_id"),
        F.last("reported_at", ignorenulls=True).over(w_claim_date_month_policy_id_b).alias("last_known_reported_at"),
    )
)

ranked = (
    with_windows
    .select(
        "*",
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_status_claim_type), 2).alias("approved_amount_vs_status_claim_type_avg"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_reported_at_to_date_claim_da)).alias("days_since_prev_claim_date"),
        F.round((F.unix_timestamp("last_known_reported_at") - F.unix_timestamp(F.lag("last_known_reported_at").over(w_reported_at_month_by_last_kn))) / 60, 1).alias("minutes_since_prev_last_known_reported_at"),
        F.last("claimed_amount").over(w_all_by_distinct_claim_type_i).alias("last_claimed_amount"),
        F.max("reported_at_dense_rank").over(w_status_claim_type).alias("status_claim_type_max_reported_at_dense_rank"),
    )
)

rollup_base = (
    ranked
    .filter(F.col("approved_amount_seq") >= 2)
    .groupBy(
        "claim_type",
        F.date_trunc("month", "reported_at").alias("reported_at_month"),
        "approved_amount_seq",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.max("distinct_claim_type_in_policy_id_adjuster_id").alias("max_distinct_claim_type_in_policy_id_adjuster_id"),
    )
    .filter(F.col("max_distinct_claim_type_in_policy_id_adjuster_id") > 4)
    .drop("max_distinct_claim_type_in_policy_id_adjuster_id")
)

result = (
    rollup_base
    .filter(F.size(F.col("claim_type_list")) >= 1)
    .select(
        "claim_type",
        "reported_at_month",
        "row_count",
        "claim_type_list",
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
    .orderBy(F.col("row_count").asc())
)
