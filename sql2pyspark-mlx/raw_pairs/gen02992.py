from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at")

result = (
    df_payments
    .filter(F.col("order_id").isin([4, 6, 12, 40]))
    .select("id", "status", "paid_at", F.rank().over(w).alias("paid_at_rank"))
    .orderBy("paid_at_rank")
)
