from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("paid_at"), "order_id").orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        "order_id",
        "amount",
        F.lag("amount", 1).over(w).alias("prev_amount"),
    )
)
