from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter(F.col("age").between(47, 64) & (F.col("country") != "DE"))
    .withColumn("is_de", F.when(F.col("country") == "DE", 1).otherwise(0))
)

agg = (
    filtered_customers
    .groupBy(
        F.when(F.col("age") >= 69, "high").when(F.col("age") >= 33, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.countDistinct("is_de").alias("unique_citys"))
)

result = (
    agg
    .orderBy(F.col("unique_citys").desc(), "age_bucket")
    .limit(5)
)
