from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id").orderBy("shipping_fee", "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc())
w4 = Window.partitionBy("store_id").orderBy(F.col("order_date").desc(), "id")

base = (
    df_orders
    .filter(F.col("order_date") >= F.to_date(F.lit("2024-02-23")))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "amount",
        "shipping_fee",
        F.upper("channel").alias("channel_upper"),
    )
)

result = (
    base
    .select(
        "*",
        F.min("shipping_fee").over(w_hist).alias("store_id_min_shipping_fee"),
        F.lead("shipping_fee", 2).over(w2).alias("next_shipping_fee"),
        F.cume_dist().over(w3).alias("shipping_fee_cume_dist"),
        F.row_number().over(w4).alias("event_seq"),
    )
)
