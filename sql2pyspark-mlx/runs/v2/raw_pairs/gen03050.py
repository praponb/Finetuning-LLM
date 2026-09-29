from pyspark.sql import functions as F

valid_products = df_products.filter((F.size("tags") >= 1) & (F.col("category") != "Home"))

products_stats = (
    valid_products
    .groupBy("supplier_id")
    .agg(F.avg("cost").alias("avg_cost"))
)

result = (
    products_stats
    .filter(F.col("avg_cost") > 490)
    .select("supplier_id", "avg_cost")
)
