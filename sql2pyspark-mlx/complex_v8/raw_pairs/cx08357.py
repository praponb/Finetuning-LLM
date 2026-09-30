from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("claim_type", "policy_id").orderBy(F.col("premium").desc(), F.col("id").desc())

joined_ranges = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.claim_type",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

latest = (
    joined_ranges
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

labeled = (
    latest
    .select(
        "*",
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.round(F.col("premium") * 1.1, 2).alias("premium_adj"),
    )
)

with_parent = (
    labeled.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team").isin("Health", "Motor", "Property")), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.adjuster_id",
        "s.premium",
        "s.claimed_amount_filled",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

result = (
    with_parent
    .filter(F.col("adjuster_level") >= 3)
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "adjuster_id",
        "claimed_amount_filled",
        "adjuster_team",
        "adjuster_name",
        "adjuster_level",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
    .orderBy("claimed_amount", F.col("claimed_amount_filled").desc())
)
