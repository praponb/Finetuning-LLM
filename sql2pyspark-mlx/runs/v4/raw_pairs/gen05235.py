from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("order_date")
w2 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id", "subscription_id").rowsBetween(-5, -1)
w3 = Window.partitionBy("status").orderBy("shipping_fee", "id", "subscription_id").rowsBetween(-5, -1)
w4 = Window.partitionBy("store_id", "channel").orderBy(F.col("amount").desc(), "id", "subscription_id")
w5 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("customer_id").orderBy(F.col("amount").asc_nulls_last())
discount_by_store_id = df_orders.groupBy("store_id").agg(F.sum("discount").alias("sum_order_discount"))

filtered = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-12-18").cast("date")) & F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        F.when(F.col("discount") >= 0.38, "High").when(F.col("discount") >= 0.15, "Medium").otherwise("Low").alias("discount_level"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

matched = (
    filtered
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & F.col("src.order_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

deduped = (
    matched
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    deduped
    .withColumn("amount_avg_5_prior", F.avg("amount").over(w2))
    .withColumn("shipping_fee_avg_5_prior", F.avg("shipping_fee").over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
)

metrics_orders = (
    metrics
    .select(
        "*",
        F.sum("amount").over(w5).alias("cumulative_amount"),
        F.rank().over(w_ordered).alias("amount_rank"),
    )
)

result = (
    metrics_orders
    .join(discount_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "shipping_fee",
        "discount_level",
        "channel_clean",
        "subscription_id",
        F.col("sum_order_discount"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
