from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("product_id").isin([5, 6])).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_anti")
    .select(F.col("p.id"), F.col("p.cost"), F.col("p.product_name"), F.col("p.brand"))
)
