from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount").desc())

result = (
    df_orders
    .filter(F.col("amount") < 1210)
    .select(
        "id",
        "channel",
        "discount",
        "shipping_fee",
        F.dense_rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
    .select("id", "channel", "discount", "shipping_fee")
    .orderBy("discount", F.col("shipping_fee").desc())
)
