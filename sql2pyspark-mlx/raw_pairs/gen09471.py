from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Firefox", "Edge", "Chrome"]) & F.col("customer_id").isin([22, 26, 33, 35]))
    .withColumn("browser_filled", F.coalesce("browser", F.lit("unassigned")))
    .withColumn("days_since_started_at", F.datediff(F.current_date(), "started_at"))
)
