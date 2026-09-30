from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "basic", "premium"]) | F.col("end_date").between("2024-02-01", "2025-06-01"))
    .withColumn("months_since_end_date", (F.datediff(F.current_date(), "end_date") / 30))
)

status_summary = (
    src
    .groupBy("status")
    .agg(F.max("end_date").alias("latest_end_date"))
)

result = status_summary.select("status", "latest_end_date")
