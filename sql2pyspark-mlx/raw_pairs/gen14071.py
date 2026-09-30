from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id")
w2 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("amount").desc())
w3 = Window.orderBy(F.col("amount").desc_nulls_first())

result = (
    df_transactions
    .filter(F.col("merchant") != "Shell")
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w)), 4))
    .withColumn("amount_rank", F.rank().over(w2))
    .withColumn("amount_percent_rank", F.percent_rank().over(w3))
)
