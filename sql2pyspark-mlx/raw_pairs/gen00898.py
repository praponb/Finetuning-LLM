from pyspark.sql import functions as F

valid_orders = df_orders.filter(F.col("store_id").isNotNull())

status_group_summary = (
    valid_orders
    .groupBy(
        F.when(F.col("status").isin(["CANCELLED", "RETURNED"]), "core").otherwise("other").alias("status_group"),
        "channel",
    )
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = status_group_summary.select("status_group", "stddev_amount")
