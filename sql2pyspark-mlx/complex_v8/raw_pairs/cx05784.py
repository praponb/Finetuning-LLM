from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_policy_id_by_clai = Window.partitionBy("claim_type", "policy_id").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claim_date_week_runni = Window.orderBy("claim_date_week", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_by_claim_date_qua = Window.partitionBy("claim_type").orderBy("claim_date_quarter", "id")
w_claim_type = Window.partitionBy("claim_type")
w_all_by_claim_date_week = Window.orderBy("claim_date_week", "id")
w_claim_date_quarter_seq_by_ap = Window.partitionBy("claim_date_quarter_seq").orderBy(F.col("approved_amount_share_of_claim_type").desc(), "id")
w_all_by_claim_date_quarter_ru = Window.orderBy(F.col("claim_date_quarter").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_status = Window.partitionBy("policy_id", "status")

base = (
    df_claims
    .filter(F.hour(F.col("reported_at")).between(7, 11))
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

annotated = (
    base
    .filter(F.col("claim_type").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "claim_date_quarter",
        F.date_trunc("week", F.col("claim_date")).alias("claim_date_week"),
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
)

ranked = (
    annotated
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "claim_date_quarter",
        "claim_date_week",
        F.first("claim_date_week").over(w_claim_type_policy_id_by_clai).alias("first_claim_date_week"),
        F.first("approved_amount", ignorenulls=True).over(w_all_by_claim_date_week_runni).alias("first_known_approved_amount"),
        F.row_number().over(w_claim_type_by_claim_date_qua).alias("claim_date_quarter_seq"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_claim_type), F.lit(0)), 4).alias("approved_amount_share_of_claim_type"),
        F.sum("claimed_amount").over(w_claim_type).alias("claim_type_sum_claimed_amount"),
        F.round((F.unix_timestamp("claim_date_quarter") - F.unix_timestamp(F.lag("claim_date_quarter").over(w_all_by_claim_date_week))) / 60, 1).alias("minutes_since_prev_claim_date_quarter"),
    )
)

windowed = (
    ranked
    .select(
        "*",
        (F.col("claim_date_quarter_seq") - F.lag("claim_date_quarter_seq").over(w_claim_date_quarter_seq_by_ap)).alias("claim_date_quarter_seq_change"),
        F.sum("first_known_approved_amount").over(w_all_by_claim_date_quarter_ru).alias("running_first_known_approved_amount"),
        F.max("claim_date_quarter_seq").over(w_policy_id_status).alias("policy_id_status_max_claim_date_quarter_seq"),
        F.sum("first_known_approved_amount").over(w_policy_id_status).alias("policy_id_status_sum_first_known_approved_amount"),
    )
)

result = (
    windowed
    .rollup("status", "claim_date_quarter_seq")
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("status").isNotNull(), F.col("minutes_since_prev_claim_date_quarter")).otherwise(0)).alias("minutes_since_prev_claim_date_quarter_status_is_"),
    )
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "claim_date_quarter_seq",
        "distinct_status",
        "n_rows",
        "minutes_since_prev_claim_date_quarter_status_is_",
    )
)
