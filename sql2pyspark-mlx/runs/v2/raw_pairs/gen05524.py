from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("amount").desc(), "id")
w2 = Window.orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "channel",
        "amount",
        "order_date",
        F.row_number().over(w).alias("amount_row"),
        F.row_number().over(w2).alias("row_num"),
    )
)
