from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("channel").orderBy("amount")

result = (
    df_orders
    .select(
        "id",
        "status",
        "amount",
        "customer_id",
        "channel",
        F.last("customer_id").over(w).alias("last_customer_id"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
