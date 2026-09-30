from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter(~F.col("city").isin(["Chiang Mai", "Bangkok"]))
    .withColumn("adjusted_age", F.round((F.col("age") * 1.07), 2))
)

summary = (
    recent_customers
    .groupBy("country")
    .agg(
        F.count("*").alias("num_customers"),
        F.avg("age").alias("avg_age"),
    )
)

result = (
    summary
    .select("country", "num_customers", "avg_age")
    .orderBy("avg_age")
)
