from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter((F.col("ss.sale_date") <= "2024-08-23") & (F.col("ss.revenue") > 1000))
    .select(F.col("ss.id"), F.col("ss.product_id"), F.col("p.launch_date"))
    .orderBy("launch_date", F.col("id").desc())
)
