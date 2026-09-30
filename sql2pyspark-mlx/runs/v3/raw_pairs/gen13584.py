from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 1)
    .select("id", "merchant", "txn_time", "is_flagged")
    .orderBy(F.col("merchant").desc(), F.col("id").desc())
)
