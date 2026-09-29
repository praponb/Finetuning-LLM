from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_payments
    .filter(F.col("amount") < 920)
    .select("id", "paid_at", "amount", F.sum("amount").over(w).alias("running_amount"))
    .orderBy("paid_at")
)
