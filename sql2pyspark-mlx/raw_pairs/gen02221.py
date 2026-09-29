from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy(F.month("effective_from")).orderBy("tier_value")
w2 = Window.partitionBy("currency")

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

scored = (
    lines
    .select(
        "*",
        F.percent_rank().over(w_running).alias("tier_value_percent_rank"),
        F.max("tier_value").over(w2).alias("currency_max_tier_value"),
    )
)

base = (
    scored
    .filter(F.col("effective_from") >= F.lit("2024-03-06").cast("date"))
    .withColumn("tier_value_whole", F.ceil("tier_value"))
)

cleaned = (
    base
    .filter(F.col("effective_from") > F.lit("2024-02-28").cast("date"))
    .select(
        "*",
        F.greatest("tier_value", "tier_value_percent_rank").alias("greatest_tier_value_tier_value_percent_rank"),
        F.coalesce("currency_max_tier_value", F.lit(0)).alias("currency_max_tier_value_filled"),
    )
)

cleaned_price_tiers = (
    cleaned
    .filter((F.col("effective_from") > F.lit("2024-03-15").cast("date")) & (F.col("tier_value_whole") == 51))
    .select(
        "*",
        F.round("currency_max_tier_value_filled", 0).alias("currency_max_tier_value_filled_rounded"),
    )
)

summary = (
    cleaned_price_tiers
    .groupBy(
        F.when(F.col("tier_value_whole") >= 88, "PREMIUM").otherwise("STANDARD").alias("tier_value_whole_tier"),
    )
    .agg(
        F.countDistinct("currency").alias("distinct_currencies"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
    )
    .filter(F.col("num_currencies") < 1000)
)

result = (
    summary
    .select("tier_value_whole_tier")
    .orderBy(F.col("tier_value_whole_tier").desc())
)
