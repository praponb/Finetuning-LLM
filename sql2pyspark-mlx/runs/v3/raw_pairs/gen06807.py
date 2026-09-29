from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at")

result = (
    df_payments
    .select("id", "order_id", "paid_at", F.rank().over(w).alias("paid_at_rank"))
    .orderBy("order_id")
)
