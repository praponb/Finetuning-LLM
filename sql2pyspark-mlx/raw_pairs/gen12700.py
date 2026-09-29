from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id")
w2 = Window.partitionBy("order_id", "method")

result = (
    df_payments
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        F.lead("amount", 1).over(w).alias("next_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
    )
)
