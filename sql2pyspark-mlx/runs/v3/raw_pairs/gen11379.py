from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device") == "tablet")
    .select("browser", "device")
    .subtract(
        df_sessions
        .filter(F.col("customer_id").isin([2, 36]))
        .select("browser", "device")
    )
)
