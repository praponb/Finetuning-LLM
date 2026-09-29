from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg(F.sum("unit_price").alias("total_unit_price"))
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("t"), F.col("products.id") == F.col("t.product_id"), "left")
    .select(
        F.col("products.id"),
        F.col("products.launch_date"),
        F.col("products.supplier_id"),
        F.col("t.total_unit_price"),
    )
)
