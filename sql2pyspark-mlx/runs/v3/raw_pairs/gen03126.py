from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time")

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 5)
)
