from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "channel").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("amount", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("channel", "status").orderBy(F.col("shipping_fee").asc_nulls_last())
w4 = Window.partitionBy("customer_id").orderBy("shipping_fee")

enriched = (
    df_orders
    .filter(F.col("amount") < 1190)
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "shipping_fee",
        F.first("discount", ignorenulls=True).over(w).alias("first_known_discount"),
        F.avg("discount").over(w2).alias("discount_avg_5_prior"),
        F.rank().over(w3).alias("shipping_fee_rank"),
        F.cume_dist().over(w4).alias("shipping_fee_cume_dist"),
    )
)

result = (
    enriched
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "shipping_fee",
        "first_known_discount",
        "discount_avg_5_prior",
        "shipping_fee_rank",
        "shipping_fee_cume_dist",
    )
    .orderBy(F.col("channel").desc())
)
