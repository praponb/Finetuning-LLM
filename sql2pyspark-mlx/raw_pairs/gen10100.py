from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "customer_id").orderBy("order_date", "id")
w2 = Window.partitionBy("status").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "channel",
        "customer_id",
        "order_date",
        "status",
        "discount",
        F.row_number().over(w).alias("row_num"),
        F.sum("discount").over(w2).alias("running_discount"),
    )
    .orderBy("order_date")
)
