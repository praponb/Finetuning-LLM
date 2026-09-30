from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter(F.col("email").like("%@startup.io") & ~F.col("segment").isin(["SMB", "Consumer"]))
    .withColumn("credit_limit_times_age", (F.col("credit_limit") * F.col("age")))
)

customers_stats = (
    filtered_customers
    .groupBy("is_active")
    .agg(F.sum("credit_limit_times_age").alias("total_credit_limit_times_age"))
)

result = (
    customers_stats
    .filter(F.col("total_credit_limit_times_age") < 240)
    .orderBy(F.col("total_credit_limit_times_age").desc())
)
