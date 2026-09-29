from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount")

result = (
    df_payments
    .select("id", "status", "amount", F.dense_rank().over(w).alias("amount_dense_rank"))
)
