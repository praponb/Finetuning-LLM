from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .select(F.col("p.launch_date"), F.col("ss.revenue"))
    .orderBy(F.col("launch_date").desc())
)
