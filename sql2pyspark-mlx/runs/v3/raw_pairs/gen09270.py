from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("amount")
w2 = Window.partitionBy("channel")

result = (
    df_orders
    .filter(F.col("customer_id") == 28)
    .select(
        "id",
        "store_id",
        "amount",
        "channel",
        F.rank().over(w).alias("rnk"),
        F.count("*").over(w2).alias("channel_count"),
    )
)
