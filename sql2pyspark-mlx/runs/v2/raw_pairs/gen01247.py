from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Firefox", "Edge", "Chrome"]) & (F.col("country") != "SG"))
    .withColumn("started_at_period", F.date_trunc("MONTH", "started_at"))
    .orderBy(F.col("id").desc())
)
