from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .withColumn("email_lower", F.lower("email"))
    .orderBy(F.col("country").desc())
)
