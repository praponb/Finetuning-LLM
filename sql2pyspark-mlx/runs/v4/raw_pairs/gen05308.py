from pyspark.sql import functions as F

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2024-04-20")))
    .select("*", F.round("quantity", 1).alias("quantity_rounded"))
)

result = (
    cleaned
    .withColumn("week_start", F.date_trunc("week", "last_restock"))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 206, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["week_start", "quantity_tier"], ["week_start"]],
        "week_start",
        "quantity_tier",
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.percentile_approx("quantity_rounded", [0.25, 0.5, 0.75]).alias("quantity_rounded_quartiles"),
        F.min("quantity_rounded").alias("min_quantity_rounded"),
        F.grouping("week_start").alias("grouping_level"),
    )
    .filter(F.col("min_quantity_rounded") > 0)
)
