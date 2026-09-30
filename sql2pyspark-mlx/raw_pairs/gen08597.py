from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("element"),
    )
)

result = (
    exploded
    .groupBy("currency")
    .agg(
        F.count("*").alias("event_count"),
        F.sum("element").alias("total_element"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("currency") == "EUR", F.col("element")).otherwise(0)).alias("eur_element"),
        F.avg(F.when(F.col("currency") == "EUR", F.col("element"))).alias("eur_avg_element"),
        F.sum(F.when(F.col("currency") == "USD", F.col("element"))).alias("usd_total_element"),
    )
)
