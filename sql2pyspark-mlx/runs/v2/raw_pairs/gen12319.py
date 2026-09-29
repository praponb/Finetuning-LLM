from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2024-01-01")
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 3)
    .orderBy(F.col("amount").desc())
)
