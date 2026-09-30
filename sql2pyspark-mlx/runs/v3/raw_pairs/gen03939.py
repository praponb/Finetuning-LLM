from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("discount").desc(), "id")
w2 = Window.partitionBy("channel")

result = (
    df_orders
    .select(
        "id",
        "discount",
        "amount",
        "channel",
        F.first("amount").over(w).alias("first_amount"),
        F.min("amount").over(w2).alias("channel_min_amount"),
    )
    .orderBy(F.col("first_amount").desc())
)
