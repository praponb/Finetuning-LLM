from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "order_date",
        "channel",
        F.first("channel").over(w).alias("first_channel"),
    )
)
