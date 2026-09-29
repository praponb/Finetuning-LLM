from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.orderBy("amount", "id")
w3 = Window.partitionBy("method")

result = (
    df_payments
    .withColumn("amount_sum_30d", F.sum("amount").over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w3)), 4))
)
