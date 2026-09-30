from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~(F.col("browser").isin(["Chrome", "Edge"]) | F.col("browser").isin(["Firefox", "Edge", "Safari"])))
    .select("country", "customer_id", F.length("device").alias("device_length"))
    .orderBy("device_length", F.col("customer_id").desc())
)
