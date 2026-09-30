from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter(~(F.col("segment").isNotNull() & (F.col("signup_date") >= "2024-11-01")))
    .withColumn("segment_lower", F.lower("segment"))
)

agg = (
    recent_customers
    .groupBy("segment")
    .agg(
        F.max("age").alias("max_age"),
        F.avg("age").alias("avg_age"),
    )
)

result = (
    agg
    .filter(F.col("avg_age") < 49)
    .select("segment", "avg_age")
    .orderBy("segment")
    .limit(10)
)
