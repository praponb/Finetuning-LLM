from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("order_id").isin([8, 12, 18, 38]))
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.unique_products") > 3)
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("ps.quantity_range"),
        F.col("ps.unique_products"),
    )
    .orderBy("quantity_range")
)
