from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country").isin(["UK", "DE", "US"]))
    .select("customer_id")
    .subtract(
        df_sessions
        .filter(F.col("started_at") < "2025-06-21")
        .select("customer_id")
    )
)
