from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id = Window.partitionBy("policy_id")
w_all_by_premium_ties = Window.orderBy("premium")

matched = (
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

with_trends = (
    matched
    .withColumn("rows_in_policy_id", F.count(F.lit(1)).over(w_policy_id))
    .withColumn("premium_rank", F.rank().over(w_all_by_premium_ties))
)

with_details_policy = (
    with_trends.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.end_date").between(F.lit("2026-01-20").cast("date"), F.lit("2026-12-05").cast("date"))), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.*",
        F.col("p.start_date").alias("policy_start_date"),
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

result = (
    with_details_policy
    .groupBy("policy_start_date", "claim_date")
    .pivot("product", ["AUTO", "TRAVEL"])
    .agg(F.avg("claimed_amount").alias("avg_v"), F.sum("claimed_amount").alias("sum_v"))
    .orderBy("policy_start_date", "claim_date")
)
