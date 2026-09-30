from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

enriched = (
    df_orders
    .withColumn("last3_max_shipping_fee", F.max("shipping_fee").over(w))
    .withColumn("last3_avg_shipping_fee", F.avg("shipping_fee").over(w))
    .withColumn("last3_sum_shipping_fee", F.sum("shipping_fee").over(w))
    .withColumn("cumulative_discount", F.sum("discount").over(w2))
)

kept = (
    enriched
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.status") == "IN_TRANSIT"), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.quantity") >= 3), "left_anti")
)

with_dims = (
    not_flagged
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.store_id",
        "src.order_date",
        "src.status",
        "src.amount",
        "src.shipping_fee",
        "src.last3_sum_shipping_fee",
        F.col("d.region"),
        F.col("d.opened_date"),
        F.col("d.city"),
    )
)

result = (
    with_dims
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "shipping_fee",
        "last3_sum_shipping_fee",
        "region",
        F.when(F.col("region").rlike("^(West|North)$"), "OTHER_ZONE").when(F.col("region").rlike("^(South|East)$"), "GROUP_B").otherwise("EXTENDED").alias("region_zone"),
        F.col("min_subscription_monthly_fee"),
    )
    .orderBy(F.col("order_date").desc_nulls_first(), F.col("region_zone").desc())
)
