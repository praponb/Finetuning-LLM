from pyspark.sql import functions as F

recent_products = df_products.filter(F.array_contains("tags", "bestseller"))

supplier_id_summary = (
    recent_products
    .groupBy("supplier_id", "category")
    .agg(F.max("launch_date").alias("last_launch_date"))
)

result = (
    supplier_id_summary
    .orderBy("category", "supplier_id")
    .limit(5)
)
