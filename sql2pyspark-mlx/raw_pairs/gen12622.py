from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", "currency", F.explode("tier_prices").alias("val"))

result = (
    exploded
    .groupBy("currency")
    .agg(
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles"),
    )
)
