from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Edge", "Chrome"]) & (F.col("browser") == "Firefox"))
    .select("id", "customer_id", "device", F.trim("browser").alias("clean_browser"))
)
