from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        "segment",
        F.when(F.col("credit_limit") >= 15300, "high").when(F.col("credit_limit") >= 5900, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.round(F.stddev("age"), 2).alias("stddev_age"))
    .filter(F.col("stddev_age") > 53)
    .select("segment", "credit_limit_bucket", "stddev_age")
)
