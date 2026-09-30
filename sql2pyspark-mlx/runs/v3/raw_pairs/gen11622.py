from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id") == 21)
    .select("id", "country", "customer_id")
    .unionByName(
        df_sessions
        .filter(F.col("country").isin(["US", "UK"]))
        .select("id", "country", "customer_id")
    )
)
