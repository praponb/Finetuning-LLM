from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id")
w2 = Window.partitionBy("status", "customer_id").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "amount",
        F.ntile(3).over(w).alias("order_date_ntile"),
        F.lead("amount", 1).over(w2).alias("next_amount"),
    )
    .orderBy("status")
)
