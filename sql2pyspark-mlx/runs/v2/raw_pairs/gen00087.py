from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 12800, "high").when(F.col("credit_limit") >= 3400, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg((F.max("age") - F.min("age")).alias("age_range"))
    .filter(F.col("age_range") < 51)
)
