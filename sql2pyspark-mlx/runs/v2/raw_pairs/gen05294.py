from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.count("*").alias("total_count"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.total_count") > 4)
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.cost"),
        F.col("t.total_count"),
    )
    .orderBy("cost")
)
