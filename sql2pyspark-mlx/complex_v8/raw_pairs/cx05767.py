from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("policy_id", "product").orderBy(F.col("approved_amount").desc(), F.col("id").desc())

joined_ranges = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.approved_amount",
        "c.status",
        "p.product",
        "p.premium",
    )
)

deduped = (
    joined_ranges
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

policy_id_stats = (
    deduped
    .groupBy("policy_id")
    .agg(F.avg("premium").alias("avg_premium"))
)

vs_group = (
    deduped.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.premium") > F.col("g.avg_premium"))
    .select(
        "s.*",
        "g.avg_premium",
        F.round(F.col("s.premium") - F.col("g.avg_premium"), 2).alias("premium_minus_avg"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "premium",
        "avg_premium",
        "premium_minus_avg",
        F.concat(F.col("product"), F.lit("#"), F.col("policy_id").cast("string")).alias("product_ref"),
    )
    .orderBy("avg_premium")
)
