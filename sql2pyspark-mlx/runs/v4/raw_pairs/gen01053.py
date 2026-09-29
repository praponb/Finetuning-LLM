from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.to_date("started_at") >= "2024-05-01")
    .select("id", "country", "customer_id")
    .unionByName(
        df_sessions
        .filter(~F.col("country").isin(["US", "DE"]))
        .select("id", "country", "customer_id")
    )
)
