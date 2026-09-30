from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~(F.col("browser").isin(["Safari", "Firefox", "Edge"]) & ~F.col("country").isin(["SG", "DE", "TH"])))
    .select("id", "country", "customer_id", "started_at")
)
