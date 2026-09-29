from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_payments
    .select(
        "id",
        "status",
        "paid_at",
        "amount",
        F.sum("amount").over(w).alias("running_amount"),
    )
)
