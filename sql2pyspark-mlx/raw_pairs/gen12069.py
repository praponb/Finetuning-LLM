from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .groupBy("product_id")
    .agg(
        F.count("*").alias("row_count"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.row_count") >= 5)
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("pt.row_count"),
        F.col("pt.avg_quantity"),
    )
    .orderBy("category", F.col("row_count").desc())
)
