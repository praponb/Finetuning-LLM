from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id")
w2 = Window.partitionBy("method").orderBy("amount")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.lead("amount", 2).over(w).alias("next_amount"),
        F.cume_dist().over(w2).alias("amount_cume_dist"),
    )
)
