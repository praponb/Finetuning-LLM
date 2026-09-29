from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "right")
    .filter(F.col("oi.unit_price") > 220)
    .groupBy(F.col("p.brand"))
    .agg(
        F.round(F.avg(F.col("oi.quantity")), 2).alias("avg_quantity"),
        F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"),
    )
    .orderBy(F.col("brand").desc())
    .limit(50)
)
