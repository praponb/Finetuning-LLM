from pyspark.sql import functions as F

result = (
    df_customers
    .agg(
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
        F.avg("age").alias("avg_age"),
    )
)
