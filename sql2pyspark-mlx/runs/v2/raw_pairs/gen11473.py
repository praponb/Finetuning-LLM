from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active") & F.col("is_active") & F.col("name").isNotNull())
    .agg(F.count("credit_limit").alias("credit_limit_count"))
    .orderBy(F.col("credit_limit_count").desc())
)
