from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id")
w2 = Window.partitionBy("status").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "order_date",
        "amount",
        "status",
        "discount",
        F.sum("amount").over(w).alias("running_amount"),
        F.lag("discount", 2).over(w2).alias("prev_discount"),
    )
)
