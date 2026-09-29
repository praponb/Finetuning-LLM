from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")

result = (
    df_orders
    .select(
        "id",
        "channel",
        "discount",
        F.sum("discount").over(w).alias("channel_sum_discount"),
    )
)
