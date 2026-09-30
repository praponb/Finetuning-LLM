from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("discount")
w2 = Window.partitionBy(F.year("order_date"), "channel").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "discount",
        "order_date",
        "channel",
        "amount",
        F.dense_rank().over(w).alias("discount_dense_rank"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
)
