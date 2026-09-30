from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.country"))
    .agg(
        F.count("*").alias("num_sessions"),
        F.sum(F.when(F.col("s.country") == "US", 1).otherwise(0)).alias("us_count"),
    )
    .filter(F.col("us_count") > 66)
    .orderBy(F.col("country").desc())
)
