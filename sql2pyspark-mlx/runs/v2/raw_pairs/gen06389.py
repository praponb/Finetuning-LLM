from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("shipping_fee").desc())
w2 = Window.partitionBy("channel")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "status",
        "shipping_fee",
        "channel",
        "discount",
        F.rank().over(w).alias("rnk"),
        F.sum("discount").over(w2).alias("channel_sum_discount"),
    )
)
