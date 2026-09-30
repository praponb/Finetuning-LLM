from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("discount").between(0.19, 0.25))
    .select(
        "id",
        "order_date",
        "amount",
        F.lag("amount", 1, 0).over(w).alias("prev_amount"),
    )
)
