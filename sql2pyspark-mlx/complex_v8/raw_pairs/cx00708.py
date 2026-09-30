from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc(), "id")
w_claim_type_group = Window.partitionBy("claim_type_group")

base_claims = (
    df_claims
    .filter(F.col("claim_date").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.when(F.col("claim_type").isin("medical", "delay"), "group_a").when(F.col("claim_type") == "theft", "group_b").otherwise("other").alias("claim_type_group"),
    )
)

windowed = (
    base_claims
    .select(
        "id",
        "policy_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "claim_type_len",
        "claim_type_group",
        F.ntile(10).over(w_all_by_approved_amount).alias("approved_amount_decile"),
        F.sum("claim_type_len").over(w_claim_type_group).alias("claim_type_group_sum_claim_type_len"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_all_by_approved_amount))) / 60, 1).alias("minutes_since_prev_reported_at"),
    )
)

filtered = (
    windowed
    .crossJoin(windowed.agg(F.min("minutes_since_prev_reported_at").alias("overall_min_minutes_since_prev_reported_at")))
    .filter(F.col("minutes_since_prev_reported_at") > F.col("overall_min_minutes_since_prev_reported_at"))
    .drop("overall_min_minutes_since_prev_reported_at")
)

result = (
    filtered
    .cube("policy_id", "approved_amount_decile")
    .agg(
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.round(100 * F.count(F.when(F.col("claim_type") != "theft", 1)) / F.count("*"), 1).alias("pct_claim_type_theft"),
    )
    .orderBy(
        F.col("policy_id").asc_nulls_last(),
        F.col("approved_amount_decile").asc_nulls_last(),
    )
)
