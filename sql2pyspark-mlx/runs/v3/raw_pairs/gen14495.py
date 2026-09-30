from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter(F.col("discount") > 0.33)
    .withColumn("channel_status", F.concat("channel", F.lit(" - "), "status"))
)

agg = (
    filtered_orders
    .groupBy(
        "status",
        F.when(F.col("status").isin(["PENDING", "RETURNED"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("amount")).otherwise(0)).alias("shipped_amount"),
    )
)

result = (
    agg
    .select("status", "status_group", "shipped_amount")
    .orderBy("status_group")
)
