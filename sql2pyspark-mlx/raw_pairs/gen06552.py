from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("week_start").orderBy(F.col("min_tier_value").desc())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("tier_value"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("week", "effective_from").alias("week_start"), "currency")
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.min("tier_value").alias("min_tier_value"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
    )
    .withColumn("min_tier_value_rank", F.rank().over(w))
)
