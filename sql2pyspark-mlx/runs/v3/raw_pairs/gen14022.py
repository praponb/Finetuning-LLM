from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(10, 18))
    .select("customer_id", "device")
    .unionByName(
        df_sessions
        .filter(F.col("started_at") >= "2024-03-19")
        .select("customer_id", "device")
    )
    .distinct()
)
