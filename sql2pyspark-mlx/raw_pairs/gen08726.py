from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("channel").orderBy("discount_plus_shipping_fee", "id").rowsBetween(-3, Window.currentRow)

valid = (
    df_orders
    .filter(F.col("status").isin(["PENDING", "CANCELLED"]) & (~F.col("channel").eqNullSafe(F.lit("web"))))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        (F.col("discount") + F.col("shipping_fee")).alias("discount_plus_shipping_fee"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

result = (
    valid
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        "discount",
        F.count("discount").over(w_part).alias("last4_count_discount"),
        F.max("discount").over(w_part).alias("last4_max_discount"),
        F.min("discount").over(w_part).alias("last4_min_discount"),
    )
)
