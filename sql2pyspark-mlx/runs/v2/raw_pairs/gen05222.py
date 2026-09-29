from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(
        F.sum(F.when(F.col("country") == "UK", F.col("credit_limit")).otherwise(0)).alias("uk_credit_limit"),
    )
    .filter(F.col("uk_credit_limit") < 27)
    .orderBy("uk_credit_limit", "is_active")
    .limit(10)
)
