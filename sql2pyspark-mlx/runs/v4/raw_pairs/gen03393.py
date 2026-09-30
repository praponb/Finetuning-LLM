from pyspark.sql import functions as F

valid_customers = (
    df_customers
    .filter(F.col("name").like("%Chaiya%"))
    .withColumn("country_lower", F.lower("country"))
)

age_bucket_summary = (
    valid_customers
    .groupBy(
        F.when(F.col("age") >= 71, "high").when(F.col("age") >= 29, "mid").otherwise("low").alias("age_bucket"),
        "city",
    )
    .agg(
        (F.max("age") - F.min("age")).alias("age_range"),
        F.count("*").alias("total_count"),
    )
)

result = age_bucket_summary.orderBy(F.col("total_count").desc())
