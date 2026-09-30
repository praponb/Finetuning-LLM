from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") < "2024-03-01")
    .select("device")
    .unionByName(
        df_sessions
        .filter(F.col("device").isin(["tablet", "desktop"]))
        .select("device")
    )
)
