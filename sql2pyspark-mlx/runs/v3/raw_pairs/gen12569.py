from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "status",
        "customer_id",
        "order_date",
        "amount",
        F.lag("amount", 1).over(w).alias("prev_amount"),
    )
)
