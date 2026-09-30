from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")

result = (
    df_orders
    .select(
        "id",
        "channel",
        "amount",
        F.sum("amount").over(w).alias("channel_sum_amount"),
    )
    .orderBy("channel_sum_amount")
)
