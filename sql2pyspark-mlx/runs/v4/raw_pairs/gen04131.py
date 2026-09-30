from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "customer_id").orderBy("order_date", "id")
w2 = Window.partitionBy("status", "store_id").orderBy("shipping_fee", "id").rowsBetween(-3, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

base = (
    df_orders
    .filter(F.col("shipping_fee") <= 21)
    .withColumn("channel_upper", F.upper("channel"))
    .withColumn("status_group", F.when(F.col("status").isin(["SHIPPED", "RETURNED"]), "Core").otherwise("Other"))
)

metrics = (
    base
    .withColumn("next_discount", F.lead("discount", 1).over(w))
    .withColumn("discount_avg_3", F.avg("discount").over(w2))
)

base_orders = (
    metrics
    .filter((F.col("order_date") >= F.to_date(F.lit("2023-04-10"))) & F.col("channel").isNotNull())
    .withColumn("order_date_label", F.date_format("order_date", "yyyy-MM-dd"))
)

result = (
    base_orders
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "channel_upper",
        "next_discount",
        F.when(F.col("channel").rlike("^(partner|web)$"), "OTHER_ZONE").when(F.col("channel").rlike("^(mobile|store)$"), "EXTENDED").otherwise("GROUP_B").alias("channel_zone"),
        F.col("avg_subscription_monthly_fee"),
    )
    .orderBy(F.col("store_id").asc_nulls_last())
)
