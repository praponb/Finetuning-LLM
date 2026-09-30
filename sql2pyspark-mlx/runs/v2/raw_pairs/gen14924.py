from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "Consumer")
    .select("id", "is_active", F.lower("country").alias("country_lower"))
    .orderBy(F.col("id").desc())
    .limit(10)
)
