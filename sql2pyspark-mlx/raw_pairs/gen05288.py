from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter(F.col("country") == "UK")
    .withColumn("adjusted_age", F.round((F.col("age") * 1.1), 2))
)

customers_stats = (
    filtered_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.round(F.stddev("age"), 2).alias("stddev_age"),
        F.count("age").alias("age_count"),
    )
)

result = (
    customers_stats
    .select("signup_quarter", "stddev_age")
    .orderBy(F.col("stddev_age").desc())
)
