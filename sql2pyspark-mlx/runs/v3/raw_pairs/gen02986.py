from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("channel").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "status",
        "amount",
        "channel",
        "order_date",
        "shipping_fee",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
        (F.col("shipping_fee") - F.lag("shipping_fee", 1, 0).over(w2)).alias("shipping_fee_change"),
    )
)
