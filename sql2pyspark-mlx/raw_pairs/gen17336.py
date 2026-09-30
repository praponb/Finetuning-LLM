from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Firefox", "Edge", "Chrome"]))
    .select("device")
    .unionByName(df_employees.select(F.col("level").alias("device")))
    .distinct()
)
