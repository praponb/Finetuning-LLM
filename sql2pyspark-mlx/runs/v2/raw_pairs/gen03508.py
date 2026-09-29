from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(12, 18))
    .select("id", "customer_id")
    .unionByName(
        df_sessions
        .filter(F.hour("started_at").between(7, 14))
        .select("id", "customer_id")
    )
    .orderBy("id")
)
