from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy(F.month("paid_at"), "order_id")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        F.max("amount").over(w).alias("order_id_max_amount"),
        F.max("amount").over(w2).alias("paid_at_month_max_amount"),
    )
)
