from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency")
w2 = Window.partitionBy("currency").orderBy("tier_value")

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("tier_value"))
)

kept_keys = (
    lines
    .select("id", "currency")
    .subtract(
        df_price_tiers
        .filter(F.col("currency").isin(["USD", "THB", "EUR"]))
        .select("id", "currency")
    )
)

verified = (
    lines
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .withColumn("plan_avg_tier_value", F.avg("tier_value").over(w))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w2))
)

kept_keys_2 = (
    with_windows
    .select("currency")
    .intersectAll(
        df_price_tiers
        .filter(F.col("plan") == "basic")
        .select("currency")
    )
)

verified_price_tiers = (
    with_windows
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

result = (
    verified_price_tiers
    .groupBy(
        F.when(F.col("tier_value_dense_rank") >= 8, "LARGE").otherwise("SMALL").alias("tier_value_dense_rank_tier"),
        "currency",
    )
    .agg(
        F.sum(F.when(F.col("plan") == "pro", F.col("tier_value")).otherwise(0)).alias("pro_tier_value"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_count"),
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value"),
    )
    .filter(F.col("pro_tier_value") < 0)
)
