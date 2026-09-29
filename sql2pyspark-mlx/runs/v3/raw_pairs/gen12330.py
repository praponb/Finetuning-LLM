from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("order_date").between("2024-06-01", "2024-11-01"))
    .select(
        "id",
        "channel",
        "order_date",
        "amount",
        (F.col("amount") - F.lag("amount", 1).over(w)).alias("amount_change"),
    )
)
