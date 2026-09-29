from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Home", "Books", "Electronics"]) | F.col("price").between(690, 1020))
    .withColumn("product_name_lower", F.lower("product_name"))
    .withColumn("launch_date_label", F.date_format("launch_date", "MMM yyyy"))
)
