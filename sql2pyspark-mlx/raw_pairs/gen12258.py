from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc(), "id")

result = (
    df_transactions
    .filter(F.col("account_id").isin([22, 29, 30, 40]))
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "txn_type", "txn_time", "is_flagged")
)
