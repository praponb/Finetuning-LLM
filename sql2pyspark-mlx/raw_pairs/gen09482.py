from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("paid_at"), "method")
w2 = Window.partitionBy(F.date_trunc("month", "paid_at"), "order_id")

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        "method",
        "amount",
        "order_id",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
        F.count("*").over(w2).alias("paid_at_period_count"),
    )
)
