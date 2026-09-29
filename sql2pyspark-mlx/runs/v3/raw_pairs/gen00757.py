from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method")
w2 = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc(), "id")

result = (
    df_payments
    .select(
        "id",
        "method",
        "amount",
        "order_id",
        "paid_at",
        F.avg("amount").over(w).alias("method_avg_amount"),
        F.row_number().over(w2).alias("paid_at_row"),
    )
)
