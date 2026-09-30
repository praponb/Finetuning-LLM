from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select("id", "country", "device")
    .unionByName(
        df_sessions
        .filter(F.hour("started_at").between(11, 19))
        .select("id", "country", "device")
    )
    .orderBy("country")
)
