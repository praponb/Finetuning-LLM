from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .filter(F.col("merchant") == "Shell")
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "merchant", "amount", "is_flagged")
    .orderBy("amount")
)
