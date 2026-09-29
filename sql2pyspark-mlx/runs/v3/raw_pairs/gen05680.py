from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("amount")

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        "amount",
        "status",
        F.sum("amount").over(w).alias("running_amount"),
        F.rank().over(w2).alias("rnk"),
    )
)
