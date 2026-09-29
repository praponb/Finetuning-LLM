from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc(), "id")

result = (
    df_transactions
    .filter(F.col("account_id").isin([13, 14, 27, 39]))
    .select("id", "account_id", "txn_time", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 2)
    .select("id", "account_id", "txn_time")
    .orderBy(F.col("txn_time").desc())
)
