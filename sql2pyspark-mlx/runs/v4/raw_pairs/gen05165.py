from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy(F.col("amount").desc(), "id")

cleaned = (
    df_transactions
    .filter(F.col("amount") <= 3800)
    .withColumn("days_since_txn_time", F.datediff(F.current_date(), "txn_time"))
)

result = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)
