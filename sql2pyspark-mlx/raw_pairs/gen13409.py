from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "channel").orderBy("order_date", "id")
w2 = Window.orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "status",
        "channel",
        "order_date",
        "discount",
        F.sum("discount").over(w).alias("running_discount"),
        F.lead("discount", 1).over(w2).alias("next_discount"),
    )
)
