from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("started_at") >= "2023-03-01") & (F.to_date("started_at") < "2024-03-15"))
)
