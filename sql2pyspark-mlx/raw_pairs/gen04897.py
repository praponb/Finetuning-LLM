from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country").isin(["SG", "UK"]) | (F.to_date("started_at") >= "2024-09-01"))
    .withColumn("is_tablet", F.when(F.col("device") == "tablet", 1).otherwise(0))
    .orderBy("device")
)
