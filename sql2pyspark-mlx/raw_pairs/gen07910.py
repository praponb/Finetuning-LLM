from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.product_name").like("w%"))
    .groupBy(F.col("p.brand"))
    .agg(
        F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"),
        F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"),
    )
    .orderBy("brand", F.col("stddev_unit_price").desc())
)
