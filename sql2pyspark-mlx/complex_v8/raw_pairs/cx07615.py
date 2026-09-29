from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy("claimed_amount", "id").rowsBetween(-3, -1)
w_claim_date_month = Window.partitionBy(F.month("claim_date"))

valid = (
    df_claims
    .filter(F.col("claim_date").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
    )
)

with_trends = (
    valid
    .select(
        "*",
        F.round(F.avg("claimed_amount").over(w_policy_id_by_claimed_amount), 2).alias("claimed_amount_avg_3_prior"),
        F.size(F.collect_set("adjuster_id").over(w_claim_date_month)).alias("distinct_adjuster_id_in_claim_date_month"),
        F.sum("claimed_amount").over(w_claim_date_month).alias("claim_date_month_sum_claimed_amount"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_claim_date_month), F.lit(0)), 4).alias("claimed_amount_share_of_claim_date_month"),
    )
)

filtered = (
    with_trends.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_date") >= F.lit("2024-04-09").cast("date")) & (F.col("c.approved_amount") <= 15857.9), "left_semi")
)

result = (
    filtered
    .groupingSets(
        [["claim_type", "distinct_adjuster_id_in_claim_date_month"], ["claim_type"], ["distinct_adjuster_id_in_claim_date_month"], []],
        "claim_type", "distinct_adjuster_id_in_claim_date_month",
    )
    .agg(F.min("reported_at").alias("first_reported_at"))
    .orderBy(
        F.col("claim_type").asc_nulls_last(),
        F.col("distinct_adjuster_id_in_claim_date_month").asc_nulls_last(),
    )
)
