from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active", "country")
    .agg(F.min("age").alias("min_age"))
    .orderBy(F.col("min_age").desc(), F.col("country").desc())
)
