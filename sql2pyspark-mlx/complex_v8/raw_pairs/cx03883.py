from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_approved_amount_runni = Window.orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_group_reported_at = Window.partitionBy("claim_type_group", F.month("reported_at"))
w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy(F.col("approved_amount").desc_nulls_first(), "id")
w_all_by_reported_at_prior_3 = Window.orderBy(F.col("reported_at").desc(), "id").rowsBetween(-3, -1)
w_status = Window.partitionBy("status")
w_all_by_reported_at_full = Window.orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_status_by_claim_da = Window.partitionBy("policy_id", "status").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_claim_type_in_claim = Window.partitionBy("distinct_claim_type_in_claim_type_group_reported", "claim_type_group")
w_claim_type_group_policy_id_b = Window.partitionBy("claim_type_group", "policy_id").orderBy(F.col("first_known_reported_at").desc(), "id")
w_all_by_distinct_claim_type_i = Window.orderBy("distinct_claim_type_in_claim_type_group_reported", "id")

recent_claims = (
    df_claims
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
        F.when(F.col("claim_type").isin("flood", "collision"), "group_a").when(F.col("claim_type") == "medical", "group_b").otherwise("other").alias("claim_type_group"),
    )
)

sequenced = (
    recent_claims
    .select(
        "*",
        F.first("reported_at", ignorenulls=True).over(w_all_by_approved_amount_runni).alias("first_known_reported_at"),
        F.size(F.collect_set("claim_type").over(w_claim_type_group_reported_at)).alias("distinct_claim_type_in_claim_type_group_reported"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_adjuster_id_by_approved_amou)).alias("days_since_prev_claim_date"),
        F.round(F.avg("approved_amount").over(w_all_by_reported_at_prior_3), 2).alias("approved_amount_avg_3_prior"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_status), 2).alias("approved_amount_vs_status_avg"),
        F.first("approved_amount").over(w_all_by_reported_at_full).alias("first_approved_amount"),
        F.last("approved_amount").over(w_policy_id_status_by_claim_da).alias("last_approved_amount"),
        F.lag("approved_amount", 3).over(w_adjuster_id_by_approved_amou).alias("prev3_approved_amount"),
    )
)

with_windows = (
    sequenced
    .select(
        "*",
        F.max("days_since_prev_claim_date").over(w_distinct_claim_type_in_claim).alias("distinct_claim_type_in_claim_type_group_reported_2"),
        F.round(F.col("days_since_prev_claim_date") - F.avg("days_since_prev_claim_date").over(w_distinct_claim_type_in_claim), 2).alias("days_since_prev_claim_date_vs_distinct_claim_typ"),
        F.round(F.avg("claimed_amount").over(w_distinct_claim_type_in_claim), 2).alias("distinct_claim_type_in_claim_type_group_reported_4"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_claim_type_group_policy_id_b))) / 60, 1).alias("minutes_since_prev_reported_at"),
        F.row_number().over(w_all_by_distinct_claim_type_i).alias("distinct_claim_type_in_claim_type_group_reported_5"),
    )
)

result = (
    with_windows
    .filter(F.to_date(F.col("reported_at")) < F.lit("2025-03-14").cast("date"))
    .select(
        "id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "claim_type_group",
        "first_known_reported_at",
        "approved_amount_avg_3_prior",
        "approved_amount_vs_status_avg",
        "last_approved_amount",
        "prev3_approved_amount",
        "distinct_claim_type_in_claim_type_group_reported_2",
        "days_since_prev_claim_date_vs_distinct_claim_typ",
        "distinct_claim_type_in_claim_type_group_reported_4",
        "minutes_since_prev_reported_at",
        "distinct_claim_type_in_claim_type_group_reported_5",
        F.round((F.unix_timestamp(F.col("reported_at")) - F.unix_timestamp(F.col("first_known_reported_at"))) / 3600, 1).alias("hours_first_known_reported_at_to_reported_at"),
    )
    .orderBy(
        F.col("distinct_claim_type_in_claim_type_group_reported_4").desc(),
        F.col("status").asc_nulls_last(),
        "id",
    )
    .limit(5)
)
