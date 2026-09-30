from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("account_id", "merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("merchant")

result = (
    df_transactions
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("amount_count_90d", F.count("amount").over(w2))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w3)), 4))
)
