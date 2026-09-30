from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 16000, "high").when(F.col("credit_limit") >= 7600, "mid").otherwise("low").alias("credit_limit_bucket"),
        "is_active",
        "country",
    )
    .agg(
        F.round(F.stddev("age"), 2).alias("stddev_age"),
        F.avg("age").alias("avg_age"),
        F.countDistinct("age").alias("unique_citys"),
    )
)
