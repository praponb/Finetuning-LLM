from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc())

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select("id", "account_id", "txn_time", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
    .orderBy(F.col("row_num").desc(), "account_id")
)
