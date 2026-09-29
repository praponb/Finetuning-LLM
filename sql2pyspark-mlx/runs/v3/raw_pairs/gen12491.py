from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .filter(F.col("amount") < 2400)
    .select("id", "merchant", "amount", F.dense_rank().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .orderBy("id", "merchant")
)
