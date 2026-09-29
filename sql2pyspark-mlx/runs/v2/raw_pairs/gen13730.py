from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "order_date",
        "amount",
        F.lag("amount", 2, 0).over(w).alias("prev_amount"),
    )
)
