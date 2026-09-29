from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id")
w2 = Window.partitionBy("status").orderBy("paid_at")

result = (
    df_payments
    .filter(F.col("amount") < 1280)
    .select(
        "id",
        "amount",
        "status",
        "paid_at",
        F.row_number().over(w).alias("row_num"),
        F.rank().over(w2).alias("paid_at_rank"),
    )
)
