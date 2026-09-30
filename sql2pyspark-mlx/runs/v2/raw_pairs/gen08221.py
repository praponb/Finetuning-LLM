from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("discount").desc())
w2 = Window.partitionBy("channel").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "discount",
        "channel",
        "order_date",
        "amount",
        F.percent_rank().over(w).alias("discount_pct_rank"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
    .orderBy("channel")
)
