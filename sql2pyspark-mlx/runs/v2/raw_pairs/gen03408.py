from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(
        F.min("credit_limit").alias("min_credit_limit"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
    .filter(F.col("max_credit_limit") >= 5700)
    .orderBy(F.col("max_credit_limit").desc(), F.col("min_credit_limit").desc())
)
