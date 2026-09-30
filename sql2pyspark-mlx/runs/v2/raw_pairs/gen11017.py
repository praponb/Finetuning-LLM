from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") < "2025-09-01")
    .select("country")
    .unionByName(df_sessions.filter(F.col("customer_id").isNull()).select("country"))
    .distinct()
    .orderBy("country")
)
