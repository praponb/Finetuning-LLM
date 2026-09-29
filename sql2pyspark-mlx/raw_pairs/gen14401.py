from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc())

result = (
    df_transactions
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 2)
)
