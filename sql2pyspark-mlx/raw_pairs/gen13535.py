from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("discount")
w2 = Window.partitionBy("channel", "store_id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "discount",
        "channel",
        "store_id",
        F.rank().over(w).alias("discount_rank"),
        F.avg("discount").over(w2).alias("channel_avg_discount"),
    )
    .orderBy("channel")
)
