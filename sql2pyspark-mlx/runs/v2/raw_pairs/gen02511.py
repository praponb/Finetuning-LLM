from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "txn_type")

result = (
    df_transactions
    .select("id", "merchant", "txn_type", F.count("*").over(w).alias("merchant_count"))
    .orderBy("merchant_count")
)
