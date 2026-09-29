from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("shipping_fee").desc())
w2 = Window.partitionBy("store_id")

result = (
    df_orders
    .select(
        "id",
        "channel",
        "shipping_fee",
        "store_id",
        "discount",
        F.percent_rank().over(w).alias("shipping_fee_pct_rank"),
        F.min("discount").over(w2).alias("store_id_min_discount"),
    )
    .orderBy("channel")
)
