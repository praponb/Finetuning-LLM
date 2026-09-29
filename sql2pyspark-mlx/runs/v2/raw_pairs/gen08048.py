from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at")

result = (
    df_payments
    .select("id", "status", "paid_at", F.dense_rank().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .select("id", "status", "paid_at")
    .orderBy(F.col("status").desc())
)
