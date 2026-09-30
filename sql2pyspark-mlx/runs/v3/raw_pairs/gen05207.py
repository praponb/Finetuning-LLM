from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.count("*").alias("store_sale_count"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .filter(F.col("pt.store_sale_count") >= 5)
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.cost"),
        F.col("pt.store_sale_count"),
    )
)
