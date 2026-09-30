from pyspark.sql import functions as F

result = (
    df_sessions
    .select("id", "device", F.upper("browser").alias("browser_upper"))
    .orderBy(F.col("id").desc())
)
