from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "channel").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-07-05").cast("date")) & F.col("status").isin(["RETURNED", "SHIPPED", "COMPLETED"]))
    .select(
        "id",
        "store_id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.coalesce("channel", F.lit("Unknown")).alias("channel_filled"),
        F.col("discount").cast("int").alias("discount_int"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

result = (
    valid
    .select(
        "id",
        "status",
        "amount",
        "shipping_fee",
        "discount_int",
        "channel_clean",
        F.max("shipping_fee").over(w).alias("running_max_shipping_fee"),
        F.avg("shipping_fee").over(w).alias("running_avg_shipping_fee"),
        F.min("shipping_fee").over(w).alias("running_min_shipping_fee"),
    )
)
