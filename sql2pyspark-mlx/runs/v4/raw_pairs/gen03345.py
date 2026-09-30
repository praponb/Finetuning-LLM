from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_last())
w_part = Window.partitionBy("currency")

flattened = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_prices").alias("element"))
)

with_windows = (
    flattened
    .withColumn("element_rank", F.rank().over(w_recent))
    .withColumn("currency_avg_element", F.avg("element").over(w_part))
)

cleaned = (
    with_windows
    .filter(F.col("currency") == "USD")
    .select(
        "id",
        "currency",
        "element",
        "currency_avg_element",
        F.coalesce("element_rank", F.lit(0)).alias("element_rank_filled"),
        F.col("currency_avg_element").cast("string").alias("currency_avg_element_num"),
    )
)

result = (
    cleaned
    .cube(
        F.when(F.col("currency_avg_element") >= 88, "HIGH").otherwise("LOW").alias("currency_avg_element_tier"),
    )
    .agg(
        F.percentile_approx("element_rank_filled", [0.25, 0.5, 0.75]).alias("element_rank_filled_quartiles"),
        F.sum(F.when(F.col("currency") == "USD", F.col("element_rank_filled")).otherwise(0)).alias("usd_element_rank_filled"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
    )
    .filter(F.col("usd_element_rank_filled") < 0)
    .orderBy(F.col("eur_rows").desc_nulls_first())
)
