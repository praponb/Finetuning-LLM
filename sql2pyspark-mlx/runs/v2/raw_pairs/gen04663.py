from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.upper(F.trim("email")) == "JACK@STARTUP.IO")
    .groupBy(
        F.when(F.col("age") >= 61, "high").when(F.col("age") >= 29, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        F.count("*").alias("customer_count"),
    )
    .filter(F.col("customer_count") >= 5)
    .orderBy("age_bucket")
)
