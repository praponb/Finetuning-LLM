from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "country", "is_active", F.length("segment").alias("segment_length"))
    .orderBy(F.col("id").desc())
    .limit(50)
)
