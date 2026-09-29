from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")

result = (
    df_payments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 3)
    .orderBy("method", F.col("amount").desc())
)
