from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["DE", "SG"]))
    .groupBy(
        F.when(F.col("age") >= 59, "high").when(F.col("age") >= 37, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.min("age").alias("min_age"),
        F.countDistinct("credit_limit").alias("unique_citys"),
    )
    .orderBy("age_bucket")
)
