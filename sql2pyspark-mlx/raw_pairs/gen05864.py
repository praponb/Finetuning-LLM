from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "UK")
    .select("id", "customer_id", "device")
    .subtract(
        df_sessions
        .filter(F.col("country").isin(["JP", "UK", "DE"]))
        .select("id", "customer_id", "device")
    )
    .orderBy("customer_id")
)
