from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.product_name"),
        F.col("oibp.row_count"),
    )
)
