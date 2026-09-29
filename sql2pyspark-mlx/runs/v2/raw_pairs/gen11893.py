from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("segment", "city", F.trim("segment").alias("clean_segment"))
)
