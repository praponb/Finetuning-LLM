from pyspark.sql import functions as F

filtered_shipments = (
    df_shipments
    .filter(F.col("cost").isNull() & ~F.col("status").isin(["LOST", "DELIVERED"]))
)

status_group_summary = (
    filtered_shipments
    .groupBy(
        F.when(F.col("status").isin(["LOST", "RETURNED"]), "tier_1").otherwise("other").alias("status_group"),
        F.when(F.col("carrier").isin(["Flash", "UPS"]), "core").otherwise("other").alias("carrier_group"),
    )
    .agg(
        F.min("delivered_date").alias("min_delivered_date"),
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("cost")).otherwise(0)).alias("delivered_cost"),
    )
)

result = (
    status_group_summary
    .filter(F.col("delivered_cost") >= 67)
    .select("status_group", "carrier_group", "min_delivered_date", "delivered_cost")
)
