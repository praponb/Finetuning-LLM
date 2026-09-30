from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("order_id").isin([4, 11, 17, 28]))
    .select("id", "paid_at", "amount", F.lead("amount", 1).over(w).alias("next_amount"))
    .orderBy("amount")
)
