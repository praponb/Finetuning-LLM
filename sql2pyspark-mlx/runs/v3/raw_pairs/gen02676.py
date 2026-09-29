from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["desktop", "tablet", "mobile"]) & F.col("browser").isNotNull())
    .withColumn("started_at_date", F.to_date("started_at"))
    .withColumn("started_at_period", F.date_trunc("year", "started_at"))
)
