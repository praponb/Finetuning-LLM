from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("paid_at"), "method").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("status").orderBy("amount", "id")

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        "method",
        "amount",
        "status",
        F.row_number().over(w).alias("rn"),
        F.ntile(2).over(w2).alias("quartile"),
    )
)
