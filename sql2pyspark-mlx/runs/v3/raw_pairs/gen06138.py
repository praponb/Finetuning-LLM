from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(
        F.min("credit_limit").alias("min_credit_limit"),
        F.max("age").alias("max_age"),
    )
    .filter(F.col("min_credit_limit") >= 7000)
    .select("is_active", "min_credit_limit", "max_age")
)
