from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc())

result = (
    df_payments
    .select("id", "status", "paid_at", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .orderBy(F.col("id").desc(), F.col("paid_at").desc())
)
