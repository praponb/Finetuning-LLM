from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .filter(F.col("order_id").isin([2, 11, 28, 29]))
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 5)
)
