from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount")

result = (
    df_payments
    .select("id", "order_id", "amount", F.dense_rank().over(w).alias("dense_rnk"))
)
