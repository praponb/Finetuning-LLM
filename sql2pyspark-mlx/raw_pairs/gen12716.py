from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("month", "paid_at"), "status")

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        "status",
        "amount",
        F.max("amount").over(w).alias("paid_at_period_max_amount"),
    )
)
