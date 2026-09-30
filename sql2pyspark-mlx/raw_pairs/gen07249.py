from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.to_date("paid_at") >= "2023-05-01")
    .select("id", "paid_at", "amount", F.lag("amount", 1).over(w).alias("prev_amount"))
    .orderBy(F.col("amount").desc(), F.col("prev_amount").desc())
)
