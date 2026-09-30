from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy(F.month("order_date"), "store_id").orderBy("amount", "id")
w_ordered = Window.orderBy(F.col("shipping_fee").asc_nulls_first())

metrics = (
    df_orders
    .select(
        "*",
        F.ntile(4).over(w_part).alias("amount_bucket"),
        F.rank().over(w_ordered).alias("shipping_fee_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "amount_bucket",
        "shipping_fee_rank",
    )
    .orderBy(F.col("discount").desc())
)
