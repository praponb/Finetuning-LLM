from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("paid_at").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .filter(F.col("order_id").isin([8, 13, 29, 34]))
    .select(
        "id",
        "paid_at",
        "order_id",
        "amount",
        F.row_number().over(w).alias("row_num"),
        F.first("paid_at").over(w2).alias("first_paid_at"),
    )
)
