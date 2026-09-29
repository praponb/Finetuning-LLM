from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") < "2025-08-27")
    .select("customer_id")
    .unionByName(
        df_sessions
        .filter(F.col("country").isin(["JP", "SG", "US"]))
        .select("customer_id")
    )
)
