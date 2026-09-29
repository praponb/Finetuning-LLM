from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("order_date").desc())
w2 = Window.partitionBy("store_id").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "channel",
        "order_date",
        "store_id",
        "amount",
        F.rank().over(w).alias("order_date_rank"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
)
