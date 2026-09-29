from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc())

result = (
    df_transactions
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 3)
)
