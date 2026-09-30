from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("age") >= 65, "high").when(F.col("age") >= 28, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.max("age").alias("max_age"))
    .filter(F.col("max_age") < 40)
    .select("age_bucket", "max_age")
    .orderBy(F.col("max_age").desc())
)
