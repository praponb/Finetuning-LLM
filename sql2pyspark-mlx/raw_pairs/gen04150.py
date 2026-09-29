from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "order_id",
        F.when(F.col("quantity") >= 8, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("row_count_2"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)
