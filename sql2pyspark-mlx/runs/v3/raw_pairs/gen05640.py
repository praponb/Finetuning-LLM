from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        "segment",
        F.when(F.col("age") >= 67, "high").when(F.col("age") >= 27, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg((F.max("age") - F.min("age")).alias("age_range"))
    .filter(F.col("age_range") >= 45)
    .orderBy("segment", "age_range", "age_bucket")
    .limit(5)
)
