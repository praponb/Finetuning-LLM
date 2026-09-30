from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .filter(F.col("amount") < 1600)
    .select(
        "id",
        "status",
        "amount",
        "paid_at",
        "method",
        F.last("paid_at").over(w).alias("last_paid_at"),
        F.row_number().over(w2).alias("rn"),
    )
)
