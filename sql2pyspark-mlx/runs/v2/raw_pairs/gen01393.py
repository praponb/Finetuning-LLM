from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Grab", "Starbucks"]))
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 5)
    .select("id", "merchant", "amount", "txn_time")
    .orderBy("merchant", "amount")
)
