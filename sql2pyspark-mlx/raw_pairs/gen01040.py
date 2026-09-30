from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.hour("started_at").between(10, 14) & F.col("device").isin(["mobile", "tablet", "desktop"])) | F.col("device").isin(["desktop", "mobile"]))
    .select("id", "browser", F.col("started_at").cast("date").alias("started_at_day"))
)
