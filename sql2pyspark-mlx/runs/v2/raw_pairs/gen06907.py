from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.count("*").alias("num_order_items"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.quantity_count") >= 9)
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("pt.quantity_count"),
        F.col("pt.num_order_items"),
    )
    .orderBy(F.col("id").desc())
)
