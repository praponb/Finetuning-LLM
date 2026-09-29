from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("order_date", "id")
w2 = Window.partitionBy(F.month("order_date"), "store_id").orderBy("order_date", "id")
w3 = Window.partitionBy("store_id", "customer_id").orderBy("channel_length", "id")
w4 = Window.partitionBy("customer_id")
w5 = Window.orderBy("channel_length", "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

cleaned = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-02-22").cast("date"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "shipping_fee",
        F.length("channel").alias("channel_length"),
        F.when(F.col("discount") >= 0.32, "High").when(F.col("discount") >= 0.15, "Medium").otherwise("Low").alias("discount_band"),
    )
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "shipping_fee",
        F.lead("shipping_fee", 2).over(w2).alias("next_shipping_fee"),
        F.lead("channel_length", 1).over(w3).alias("next_channel_length"),
        F.min("shipping_fee").over(w4).alias("customer_id_min_shipping_fee"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

enriched_orders = (
    enriched
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.shipping_fee",
        "a.customer_id_min_shipping_fee",
        "a.row_seq",
        F.col("c.country"),
        F.col("c.name"),
    )
)

result = (
    enriched_orders
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "shipping_fee",
        "customer_id_min_shipping_fee",
        "row_seq",
        "country",
        F.when(F.col("country").rlike("^(UK|JP)$"), "GROUP_B").when(F.col("country").rlike("^(TH|US)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("country_zone"),
        F.col("max_subscription_monthly_fee"),
    )
    .orderBy(F.col("max_subscription_monthly_fee").desc_nulls_last())
)
