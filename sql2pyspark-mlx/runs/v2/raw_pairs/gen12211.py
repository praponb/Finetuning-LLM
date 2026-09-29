from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(11, 18) & (F.col("browser").isin(["Chrome", "Safari", "Firefox"]) | (F.to_date("started_at") >= "2025-08-01")))
    .select("id", "browser", "customer_id")
)
