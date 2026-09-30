from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date")
w2 = Window.partitionBy("status", "channel").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "discount",
        F.rank().over(w).alias("order_date_rank"),
        F.lead("discount", 2).over(w2).alias("next_discount"),
    )
)
