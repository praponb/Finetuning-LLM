from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("amount") >= 1310)
    .select("id", "paid_at", "amount", F.lag("amount", 2).over(w).alias("prev_amount"))
)
