from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Chrome", "Edge", "Firefox"]) & (F.col("browser") != "Edge"))
    .select("customer_id", "device", F.upper("browser").alias("browser_upper"))
    .orderBy("customer_id")
)
