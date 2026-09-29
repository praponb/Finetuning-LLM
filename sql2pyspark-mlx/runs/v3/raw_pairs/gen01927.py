from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device") == "desktop")
    .select("id", "customer_id", "browser")
    .subtract(
        df_sessions
        .filter(F.col("country") != "UK")
        .select("id", "customer_id", "browser")
    )
    .orderBy(F.col("id").desc())
)
