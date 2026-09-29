from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("channel") == "store")
    .select(
        "id",
        "channel",
        "order_date",
        "shipping_fee",
        (F.col("shipping_fee") - F.lag("shipping_fee", 1).over(w)).alias("shipping_fee_change"),
    )
)
