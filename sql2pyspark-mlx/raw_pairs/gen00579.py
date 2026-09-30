from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["North", "South", "Central"]))
    .select("region")
    .subtract(
        df_web_events
        .filter(F.col("session_id").isin([12, 30, 38]))
        .select(F.col("page").alias("region"))
    )
    .orderBy("region")
)
