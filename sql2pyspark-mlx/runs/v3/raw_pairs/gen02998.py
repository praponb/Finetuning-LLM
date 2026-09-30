from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc())

result = (
    df_payments
    .select("id", "method", "amount", F.dense_rank().over(w).alias("amount_dense_rank"))
)
