from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("age") >= 62, "high").when(F.col("age") >= 33, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg((F.max("age") - F.min("age")).alias("age_range"))
)
