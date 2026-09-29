from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id", "product_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
    )
    .filter(F.col("quantity_p95") > 0)
    .orderBy(F.col("order_id").desc_nulls_first())
)
