from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser") != "Chrome")
    .withColumn("started_at_period", F.date_trunc("year", "started_at"))
    .withColumn("is_edge", F.when(F.col("browser") == "Edge", 1).otherwise(0))
)
