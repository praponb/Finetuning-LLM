from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 5)
    .orderBy("account_id")
)
