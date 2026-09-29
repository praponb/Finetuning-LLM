from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .groupBy(F.col("p.brand"))
    .agg(F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"))
    .orderBy("brand", "stddev_unit_price")
)
