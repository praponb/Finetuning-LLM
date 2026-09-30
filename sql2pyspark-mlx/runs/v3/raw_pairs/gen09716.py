from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("method", "order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_payments
    .filter(F.to_date("paid_at") < "2023-07-01")
    .select(
        "id",
        "status",
        "method",
        "order_id",
        "paid_at",
        "amount",
        F.count("*").over(w).alias("status_count"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
)
