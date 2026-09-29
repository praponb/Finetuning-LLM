from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "status").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("customer_id") == 30)
    .select(
        "id",
        "channel",
        "status",
        "order_date",
        "amount",
        F.lag("amount", 2).over(w).alias("prev_amount"),
    )
    .orderBy("channel")
)
