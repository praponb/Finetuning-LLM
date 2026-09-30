from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") >= "2025-04-16")
    .select("id", "browser", "customer_id")
    .unionByName(
        df_sessions
        .filter(F.col("country") == "TH")
        .select("id", "browser", "customer_id")
    )
    .distinct()
    .orderBy(F.col("customer_id").desc())
)
