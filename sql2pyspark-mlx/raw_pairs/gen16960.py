from pyspark.sql import functions as F

enriched = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.shipped_date",
        F.col("d.discount"),
        F.col("d.channel"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("week", "shipped_date").alias("week_start"))
    .agg(
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
        F.round(F.sum("discount") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("discount_per_partner"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("discount_per_partner").desc())
)
