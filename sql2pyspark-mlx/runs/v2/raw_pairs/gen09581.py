from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")

result = (
    df_transactions
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 2)
    .select("id", "account_id", "txn_time")
)
