from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("browser").isin(["Firefox", "Safari"]))
    .select("id", "customer_id", "started_at")
    .orderBy(F.col("customer_id").desc(), F.col("started_at").desc())
)
