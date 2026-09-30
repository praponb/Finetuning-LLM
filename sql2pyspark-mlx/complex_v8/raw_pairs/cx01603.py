from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_reported_at = Window.partitionBy("policy_id").orderBy(F.col("reported_at").asc_nulls_last(), "id")
w_adjuster_id = Window.partitionBy("adjuster_id")

filtered_claims = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
    )
)

screened = (
    filtered_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_type") == "flood") & (F.col("c.approved_amount") >= 5336.7), "left_semi")
)

metrics = (
    screened
    .select(
        "*",
        F.row_number().over(w_policy_id_by_reported_at).alias("reported_at_seq"),
        F.rank().over(w_policy_id_by_reported_at).alias("reported_at_rank"),
        F.round(F.cume_dist().over(w_policy_id_by_reported_at), 3).alias("reported_at_cume_dist"),
        F.sum("claimed_amount").over(w_adjuster_id).alias("adjuster_id_sum_claimed_amount"),
    )
)

enriched = (
    metrics
    .filter(F.col("claimed_amount") >= 17585.2)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "reported_at_rank",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
    )
)

top_rows = (
    enriched
    .filter(F.col("reported_at_rank") <= 2)
    .drop("reported_at_rank")
)

result = (
    top_rows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "reported_at",
        "days_since_reported_at",
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
        F.date_format(F.col("reported_at"), "yyyy-MM-dd").alias("reported_at_label"),
    )
    .orderBy("adjuster_id", "id")
    .limit(20)
)
