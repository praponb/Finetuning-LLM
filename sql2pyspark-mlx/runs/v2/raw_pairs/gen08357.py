from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount", "id")

result = (
    df_payments
    .filter(F.col("order_id").isNull())
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "method", "amount", "paid_at")
    .orderBy("paid_at", F.col("id").desc())
)
