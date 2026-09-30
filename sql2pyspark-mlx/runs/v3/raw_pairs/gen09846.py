from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("product_id").isin([35, 38]))
    .groupBy("product_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("p.supplier_id"),
        F.col("t.max_quantity"),
        F.col("t.quantity_range"),
    )
)
