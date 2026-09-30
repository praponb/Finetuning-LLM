from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.to_date("started_at") >= "2023-09-01") & (F.col("browser") == "Edge"))
)
