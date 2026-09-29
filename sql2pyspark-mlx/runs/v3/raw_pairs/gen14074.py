from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(F.col("cost") <= 68)

status_group_summary = (
    recent_shipments
    .groupBy(
        F.when(F.col("status").isin(["DELIVERED", "RETURNED"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_count"))
)

result = status_group_summary.select("status_group", "fedex_count")
