from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc(), "id")

result = (
    df_transactions
    .filter(F.col("amount") >= 2600)
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 3)
)
