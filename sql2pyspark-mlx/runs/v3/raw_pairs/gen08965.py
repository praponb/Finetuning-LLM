from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc())

result = (
    df_payments
    .select("id", "status", "amount", "method", F.dense_rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 5)
    .select("id", "status", "amount", "method")
)
