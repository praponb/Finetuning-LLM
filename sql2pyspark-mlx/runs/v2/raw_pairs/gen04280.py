from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & ~F.col("device").isin(["mobile", "tablet"]))
    .withColumn("browser_filled", F.coalesce("browser", F.lit("N/A")))
    .withColumn("browser_title", F.initcap(F.trim("browser")))
)
