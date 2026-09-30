from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("age") >= 59, "high").when(F.col("age") >= 26, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.sum("age").alias("total_age"),
        F.countDistinct("age").alias("unique_citys"),
    )
    .filter(F.col("unique_citys") >= 7)
    .select("age_bucket", "total_age", "unique_citys")
    .orderBy("total_age", "age_bucket")
    .limit(5)
)
