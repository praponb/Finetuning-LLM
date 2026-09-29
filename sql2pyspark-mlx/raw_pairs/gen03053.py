from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") < 250)
    .select("brand", "launch_date", F.lower("product_name").alias("product_name_lower"))
)
