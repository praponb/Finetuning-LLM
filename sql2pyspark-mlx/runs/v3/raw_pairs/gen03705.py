from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount")

result = (
    df_transactions
    .filter(F.col("account_id") == 3)
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") == 1)
)
