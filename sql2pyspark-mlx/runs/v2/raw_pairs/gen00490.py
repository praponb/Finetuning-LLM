from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("launch_date") >= "2025-08-13") & (F.col("category") != "Home"))
    .select("id", "product_name")
    .orderBy(F.col("id").desc(), "product_name")
)
