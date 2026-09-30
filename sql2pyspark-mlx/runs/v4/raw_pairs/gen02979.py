from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "status",
        "amount",
        "order_id",
        "paid_at",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
        F.row_number().over(w2).alias("rn"),
    )
    .orderBy(F.col("amount").desc())
)
