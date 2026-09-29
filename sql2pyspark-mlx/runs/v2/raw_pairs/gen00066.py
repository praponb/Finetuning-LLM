from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["desktop", "tablet", "mobile"]))
    .select("id", "device", "customer_id")
    .unionByName(
        df_sessions
        .filter(F.hour("started_at").between(12, 20))
        .select("id", "device", "customer_id")
    )
    .orderBy("customer_id")
)
