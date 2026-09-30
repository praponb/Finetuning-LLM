from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("age") >= 70, "high").when(F.col("age") >= 35, "mid").otherwise("low").alias("age_bucket"),
        "city",
    )
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") >= 2)
    .orderBy("row_count")
)
