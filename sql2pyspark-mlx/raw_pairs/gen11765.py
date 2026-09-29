from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("paid_at") >= "2023-04-01")
    .select(
        "id",
        "amount",
        "method",
        "paid_at",
        F.row_number().over(w).alias("row_num"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
    .orderBy("amount")
)
