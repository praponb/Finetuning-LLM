from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("product_id") == 40)
    .groupBy("product_id")
    .agg(
        F.count("*").alias("num_order_items"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.num_order_items") > 8)
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("p.supplier_id"),
        F.col("t.num_order_items"),
        F.col("t.quantity_count"),
    )
    .orderBy(F.col("id").desc())
)
