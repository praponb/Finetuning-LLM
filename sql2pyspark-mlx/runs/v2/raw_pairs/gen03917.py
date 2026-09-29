from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.length("name") < 12)
    .agg(F.count("age").alias("age_count"))
    .orderBy(F.col("age_count").desc())
)
