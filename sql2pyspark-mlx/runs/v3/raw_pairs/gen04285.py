from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .union(df_store_sales.select("sale_date"))
    .distinct()
    .orderBy(F.col("launch_date").desc())
)
