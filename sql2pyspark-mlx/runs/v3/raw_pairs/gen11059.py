from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at")

result = (
    df_payments
    .filter(F.col("paid_at") >= "2024-08-12")
    .select("id", "order_id", "paid_at", F.rank().over(w).alias("rnk"))
)
