from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("element"),
    )
)

result = (
    flattened
    .groupBy("currency", F.date_trunc("WEEK", "effective_from").alias("week_start"))
    .agg(
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("element_per_usd"),
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("element_per_usd_2"),
        F.max("element").alias("max_element"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
    )
    .orderBy(F.col("usd_rows").desc_nulls_last())
)
