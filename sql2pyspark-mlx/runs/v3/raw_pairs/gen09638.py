from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.to_date("started_at") >= "2024-01-01")
    .select("started_at", F.initcap(F.trim("device")).alias("device_title"))
    .orderBy(F.col("started_at").desc())
)
