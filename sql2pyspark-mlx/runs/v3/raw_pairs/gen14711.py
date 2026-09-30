from pyspark.sql import functions as F

recent_products = df_products.filter(F.col("supplier_id") == 7)

category_summary = (
    recent_products
    .groupBy("category")
    .agg(
        F.min("price").alias("min_price"),
        F.count("*").alias("total_count"),
    )
)

result = category_summary.orderBy("category", "total_count")
