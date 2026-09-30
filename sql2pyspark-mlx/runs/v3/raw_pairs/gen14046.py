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
        F.lag("amount", 2).over(w).alias("prev_amount"),
    )
)
