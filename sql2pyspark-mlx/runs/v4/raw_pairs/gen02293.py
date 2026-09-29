from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc())

result = (
    df_payments
    .select("id", "method", "amount", "paid_at", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "method", "amount", "paid_at")
)
