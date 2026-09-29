from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .filter(F.col("product_id") == 25)
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "inner")
    .filter(F.col("oibp.total_quantity") > 31)
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("p.launch_date"),
        F.col("oibp.total_quantity"),
    )
)
