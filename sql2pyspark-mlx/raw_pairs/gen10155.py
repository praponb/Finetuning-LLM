from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "DE")
    .select("device")
    .intersect(
        df_sessions
        .filter(F.to_date("started_at") < "2023-03-01")
        .select("device")
    )
)
