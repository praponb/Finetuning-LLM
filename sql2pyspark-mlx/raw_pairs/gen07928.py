from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~(~F.col("browser").isin(["Chrome", "Safari"]) & (F.col("browser") == "Firefox") & (F.col("started_at") < "2024-08-01")))
    .select("id", "browser", "customer_id", "device")
    .orderBy(F.col("id").desc())
)
