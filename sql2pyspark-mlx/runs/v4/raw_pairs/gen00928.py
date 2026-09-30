from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at")

result = (
    df_payments
    .filter(F.col("amount") <= 990)
    .select("id", "method", "paid_at", "amount", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "method", "paid_at", "amount")
    .orderBy(F.col("amount").desc())
)
