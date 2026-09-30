from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id")
w2 = Window.partitionBy("channel").orderBy(F.col("discount").desc(), "id")

result = (
    df_orders
    .filter(F.year("order_date") >= 2024)
    .select(
        "id",
        "channel",
        "order_date",
        "amount",
        "discount",
        (F.col("amount") - F.lag("amount", 1).over(w)).alias("amount_change"),
        F.row_number().over(w2).alias("rn"),
    )
)
