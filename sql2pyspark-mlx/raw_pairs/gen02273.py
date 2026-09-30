from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").desc())
w_recent = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_first())
w3 = Window.orderBy(F.col("tier_value_percent_rank").asc_nulls_first())
w4 = Window.orderBy(F.col("tier_value").desc())
w5 = Window.orderBy(F.col("tier_value_percent_rank").desc_nulls_first())
w6 = Window.orderBy(F.col("tier_value_percent_rank").desc())
w7 = Window.orderBy("tier_value_percent_rank_cume_dist")

exploded = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("tier_limits").alias("tier_value"))
)

metrics = (
    exploded
    .filter(F.col("tier_value") < 78)
    .select(
        "id",
        "tier_value",
        F.rank().over(w).alias("tier_value_rank"),
        F.percent_rank().over(w_recent).alias("tier_value_percent_rank"),
    )
)

enriched = (
    metrics
    .withColumn("tier_value_percent_rank_cume_dist", F.cume_dist().over(w3))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w4))
    .withColumn("tier_value_percent_rank_dense_rank", F.dense_rank().over(w5))
)

metrics_price_tiers = (
    enriched
    .select(
        "*",
        F.cume_dist().over(w6).alias("tier_value_percent_rank_cume_dist_2"),
        F.dense_rank().over(w7).alias("tier_value_percent_rank_cume_dist_dense_rank"),
    )
)

kept_keys = (
    metrics_price_tiers
    .select("id")
    .exceptAll(
        df_price_tiers
        .filter(F.col("plan").isin(["enterprise", "basic"]))
        .select("id")
    )
)

verified = (
    metrics_price_tiers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "tier_value",
        "tier_value_percent_rank",
        "tier_value_percent_rank_cume_dist",
        "tier_value_dense_rank",
        "tier_value_percent_rank_dense_rank",
        "tier_value_percent_rank_cume_dist_2",
        "tier_value_percent_rank_cume_dist_dense_rank",
    )
)
