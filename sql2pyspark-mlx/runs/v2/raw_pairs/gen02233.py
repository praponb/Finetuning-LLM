from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("amount", "id")

result = (
    df_payments
    .select(
        "id",
        "method",
        "paid_at",
        "amount",
        F.row_number().over(w).alias("paid_at_row"),
        F.row_number().over(w2).alias("amount_row"),
    )
    .orderBy("amount")
)
