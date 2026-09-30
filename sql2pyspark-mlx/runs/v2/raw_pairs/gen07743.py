from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .filter(F.col("quantity") == 8)
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "left")
    .filter(F.col("oibp.avg_quantity") > 5.1)
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("p.cost"),
        F.col("oibp.avg_quantity"),
    )
    .orderBy("cost")
)
