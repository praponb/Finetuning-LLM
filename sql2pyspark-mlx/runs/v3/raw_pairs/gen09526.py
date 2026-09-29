from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "status",
        "order_date",
        "amount",
        (F.col("amount") - F.lag("amount", 1, 0).over(w)).alias("amount_change"),
    )
)
