from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") <= "2024-02-01")
    .groupBy(
        "status",
        F.when(F.col("channel").isin(["mobile", "partner"]), "group_a").otherwise("other").alias("channel_group"),
    )
    .agg(
        F.max("order_date").alias("last_order_date"),
        F.sum("discount").alias("total_discount"),
        F.min("order_date").alias("min_order_date"),
    )
    .orderBy(F.col("status").desc(), "last_order_date")
)
