from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select("id", "customer_id")
    .subtract(
        df_sessions
        .filter(F.col("country").isin(["TH", "UK", "DE"]))
        .select("id", "customer_id")
    )
)
