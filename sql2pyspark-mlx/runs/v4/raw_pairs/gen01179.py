from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "channel",
        "order_date",
        "discount",
        F.lag("discount", 1).over(w).alias("prev_discount"),
    )
    .orderBy("order_date")
)
