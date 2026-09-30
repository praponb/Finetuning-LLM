from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at")

result = (
    df_payments
    .filter(F.col("amount") > 1050)
    .select("id", "paid_at", F.dense_rank().over(w).alias("paid_at_dense_rank"))
)
