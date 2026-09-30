from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

enriched = (
    exploded
    .select(
        "currency",
        "tier_value",
        F.cume_dist().over(w).alias("tier_value_cume_dist"),
        F.percent_rank().over(w2).alias("tier_value_percent_rank"),
    )
)

cleaned = (
    enriched
    .filter(F.col("tier_value").between(41, 54))
    .withColumn("tier_value_percent_rank_times_tier_value", (F.col("tier_value_percent_rank") * F.col("tier_value")))
)

cleaned_price_tiers = (
    cleaned
    .filter(F.col("tier_value_percent_rank") <= 0.63)
    .withColumn("tier_value_percent_rank_int", F.col("tier_value_percent_rank").cast("int"))
)

result = (
    cleaned_price_tiers
    .select(
        "tier_value",
        "tier_value_cume_dist",
        "tier_value_percent_rank",
        "tier_value_percent_rank_times_tier_value",
        "tier_value_percent_rank_int",
    )
    .orderBy(F.col("tier_value_percent_rank_int").desc())
)
