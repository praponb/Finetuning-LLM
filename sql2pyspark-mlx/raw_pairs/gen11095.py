from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.avg("unit_price").alias("avg_unit_price"),
        F.min("unit_price").alias("min_unit_price"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
    )
    .filter(F.col("corr_quantity_unit_price") >= 0)
    .orderBy(F.col("corr_quantity_unit_price").desc_nulls_last())
)
