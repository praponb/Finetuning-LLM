from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc(), "id")

result = (
    df_transactions
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 5)
    .orderBy("txn_type")
)
