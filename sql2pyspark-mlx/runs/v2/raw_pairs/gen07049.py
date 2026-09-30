from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2025-07-18")
    .select("id", "merchant", "txn_time", F.ntile(4).over(w).alias("quartile"))
)
