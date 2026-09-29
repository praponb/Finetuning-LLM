from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") <= "2024-08-01")
    .groupBy(
        F.when(F.col("status").isin(["IN_TRANSIT", "DELIVERED"]), "priority").otherwise("other").alias("status_group"),
        "carrier",
    )
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
    .orderBy("carrier")
)
