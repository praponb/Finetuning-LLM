from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & F.col("channel").isin(["store", "web"]))
    .select(
        "id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.least("amount", "discount").alias("least_amount_discount"),
        F.round("amount", 1).alias("amount_rounded"),
    )
)

result = (
    base
    .withColumn("amount_rounded_tier", F.when(F.col("amount_rounded") >= 71, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["status", "amount_rounded_tier"], ["status"], []],
        "status",
        "amount_rounded_tier",
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
        F.grouping_id().alias("grouping_level"),
    )
)
