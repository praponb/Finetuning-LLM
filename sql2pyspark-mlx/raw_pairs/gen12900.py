from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id")
w2 = Window.partitionBy("channel")
w3 = Window.partitionBy(F.month("order_date"), "channel")
discount_by_store_id = df_orders.groupBy("store_id").agg(F.avg("discount").alias("avg_order_discount"))

metrics = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "shipping_fee",
        F.row_number().over(w).alias("event_seq"),
        F.corr("shipping_fee", "amount").over(w2).alias("corr_shipping_fee_amount"),
        F.corr("shipping_fee", "amount").over(w3).alias("corr_shipping_fee_amount_2"),
    )
)

result = (
    metrics
    .join(discount_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "order_date",
        "shipping_fee",
        "event_seq",
        "corr_shipping_fee_amount_2",
        F.col("avg_order_discount"),
    )
)
