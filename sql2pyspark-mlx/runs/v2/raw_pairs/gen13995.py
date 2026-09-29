from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") >= "2025-10-01")
    .select("id", "customer_id", "browser")
    .subtract(
        df_sessions
        .filter(F.col("device").isin(["desktop", "mobile"]))
        .select("id", "customer_id", "browser")
    )
)
