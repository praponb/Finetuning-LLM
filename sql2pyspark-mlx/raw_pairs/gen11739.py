from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id") == 33)
    .select("id", "device", "customer_id")
    .unionByName(
        df_sessions
        .filter(F.col("device") == "mobile")
        .select("id", "device", "customer_id")
    )
)
