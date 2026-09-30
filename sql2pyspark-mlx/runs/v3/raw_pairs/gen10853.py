from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .select(F.col("ss.id"), F.col("ss.revenue"), F.col("p.cost"), F.col("p.price"))
    .orderBy(F.col("revenue").desc())
)
