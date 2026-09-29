from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("method", "status").orderBy("amount", "id")
w_recent = Window.partitionBy("order_id").orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        F.lag("amount").over(w_ordered).alias("prev_amount"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.lead("amount", 1).over(w_recent).alias("next_amount"),
    )
)
