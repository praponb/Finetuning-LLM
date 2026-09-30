from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser") != "Firefox")
    .select("id", "customer_id", "device")
    .subtract(
        df_sessions
        .filter(F.hour("started_at").between(12, 15))
        .select("id", "customer_id", "device")
    )
    .orderBy("customer_id")
)
