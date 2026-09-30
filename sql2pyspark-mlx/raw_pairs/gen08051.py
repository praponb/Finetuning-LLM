from pyspark.sql import functions as F

result = (
    df_sessions
    .select("id", "customer_id")
    .subtract(
        df_sessions
        .filter(F.col("country") == "DE")
        .select("id", "customer_id")
    )
)
