from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter((F.col("delivered_date") < "2023-07-01") & (F.col("shipped_date") > "2024-04-01") & ~F.col("carrier").isin(["UPS", "Flash"]))
)

status_summary = (
    valid_shipments
    .groupBy("status", F.date_trunc("MONTH", "shipped_date").alias("shipped_month"))
    .agg(F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_count"))
)

result = status_summary.select("status", "shipped_month", "lost_count")
