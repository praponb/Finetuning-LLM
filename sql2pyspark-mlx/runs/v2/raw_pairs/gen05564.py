from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 5)
)
