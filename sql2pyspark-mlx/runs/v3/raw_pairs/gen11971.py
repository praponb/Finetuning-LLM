from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at")

result = (
    df_payments
    .select("id", "order_id", "paid_at", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "paid_at")
)
