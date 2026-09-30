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
    .cube(F.date_trunc("week", "effective_from").alias("week_start"))
    .agg(
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
    )
)
