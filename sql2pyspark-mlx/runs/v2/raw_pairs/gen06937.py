from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        "country",
        F.when(F.col("credit_limit") >= 16400, "high").when(F.col("credit_limit") >= 4400, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", F.col("age")).otherwise(0)).alias("bangkok_age"),
    )
    .filter(F.col("bangkok_age") > 39)
)
