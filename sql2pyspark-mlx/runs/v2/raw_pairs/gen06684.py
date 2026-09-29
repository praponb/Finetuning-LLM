from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.to_date("started_at") < "2024-12-01")
    .select("id", "started_at", F.year("started_at").alias("started_at_year"))
)
