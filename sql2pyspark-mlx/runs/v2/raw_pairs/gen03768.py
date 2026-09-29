from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount")

result = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "REFUNDED", "SUCCESS"]))
    .select("id", "status", "amount", "paid_at", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 5)
    .select("id", "status", "amount", "paid_at")
)
