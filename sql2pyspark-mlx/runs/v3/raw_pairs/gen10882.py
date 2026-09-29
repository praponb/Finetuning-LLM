from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category") == "Books") & F.col("launch_date").between("2024-10-07", "2025-06-01"))
    .withColumn("brand_length", F.length("brand"))
)
