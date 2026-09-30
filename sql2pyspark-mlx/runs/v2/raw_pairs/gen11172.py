from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "clearance"))
    .select(F.col("ss.id"), F.col("ss.revenue"), F.col("ss.units"), F.col("p.price"))
)
