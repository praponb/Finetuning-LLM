from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy(F.date_trunc("YEAR", "paid_at")).orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "paid_at",
        "amount",
        F.lag("amount", 1).over(w).alias("prev_amount"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
)
