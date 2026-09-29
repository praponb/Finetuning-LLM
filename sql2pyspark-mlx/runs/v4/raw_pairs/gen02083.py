from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Chrome", "Edge", "Firefox"]) & (F.col("browser") == "Safari") & (F.col("customer_id") == 28))
    .select("started_at")
)
