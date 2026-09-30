from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isin([15, 18, 21]) & F.col("browser").isin(["Chrome", "Safari"]) & F.col("browser").isNotNull())
    .withColumn("country_length", F.length("country"))
    .withColumn("device_prefix", F.substring("device", 1, 3))
    .orderBy("started_at", "id")
    .limit(5)
)
