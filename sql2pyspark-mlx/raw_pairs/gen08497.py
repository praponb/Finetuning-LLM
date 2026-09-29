from pyspark.sql import functions as F

base = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-04-27 17:00:00").cast("timestamp"))
    .select("*", F.length("browser").alias("browser_length"))
)

result = (
    base
    .select("browser", "customer_id")
    .subtract(
        df_sessions
        .filter(F.col("browser").isin(["Firefox", "Edge", "Safari"]))
        .select("browser", "customer_id")
    )
)
