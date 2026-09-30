from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("element"),
    )
)

result = (
    exploded
    .groupBy(F.date_trunc("QUARTER", "effective_from").alias("quarter_start"))
    .agg(
        F.sum(F.when(F.col("currency") == "EUR", F.col("element")).otherwise(0)).alias("eur_element"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.sum(F.when(F.col("currency") == "USD", F.col("element")).otherwise(0)).alias("usd_element"),
    )
)
