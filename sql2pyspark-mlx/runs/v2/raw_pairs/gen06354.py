from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(
        F.sum("age").alias("total_age"),
        F.count("credit_limit").alias("credit_limit_count"),
        F.sum("credit_limit").alias("total_credit_limit"),
        F.count("age").alias("age_count"),
    )
    .filter(F.col("age_count") >= 2)
    .drop("age_count")
)
