from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("shipping_fee", "id")
w2 = Window.partitionBy("customer_id").orderBy("shipping_fee", "id").rowsBetween(-2, -1)
w_running = Window.partitionBy("status", "store_id")
w4 = Window.partitionBy("status").orderBy(F.col("shipping_fee").asc_nulls_last())
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

with_windows = (
    df_orders
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.row_number().over(w).alias("event_seq"),
        F.avg("amount").over(w2).alias("amount_avg_2_prior"),
        F.avg("discount").over(w_running).alias("status_avg_discount"),
        F.rank().over(w4).alias("shipping_fee_rank"),
    )
)

result = (
    with_windows
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "amount_avg_2_prior",
        "status_avg_discount",
        F.when(F.col("channel").rlike("^(mobile|partner)$"), "CORE").otherwise("OTHER_ZONE").alias("channel_zone"),
        F.col("avg_subscription_monthly_fee"),
    )
    .orderBy(F.col("customer_id").desc())
)
