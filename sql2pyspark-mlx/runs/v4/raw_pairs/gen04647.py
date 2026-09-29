from pyspark.sql import functions as F

recent_products = df_products.filter((F.col("brand") == "Acme") & F.col("supplier_id").isNotNull())

category_summary = (
    recent_products
    .groupBy("category")
    .agg(F.avg("price").alias("avg_price"))
)

result = (
    category_summary
    .orderBy(F.col("avg_price").desc(), "category")
    .limit(5)
)
