from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["desktop", "tablet", "mobile"]))
    .select("id", "country", "customer_id")
    .intersect(
        df_sessions
        .filter(F.col("device") == "mobile")
        .select("id", "country", "customer_id")
    )
)
