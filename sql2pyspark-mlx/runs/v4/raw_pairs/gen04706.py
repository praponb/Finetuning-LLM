from pyspark.sql import functions as F

result = (
    df_sessions
    .select("country", "device")
    .exceptAll(
        df_sessions
        .filter(F.col("customer_id").isin([11, 21, 27, 31]))
        .select("country", "device")
    )
)
