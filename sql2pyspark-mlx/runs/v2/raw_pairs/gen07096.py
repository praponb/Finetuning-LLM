from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "order_date",
        "amount",
        F.lag("amount", 1, 0).over(w).alias("prev_amount"),
    )
    .orderBy("id")
)
