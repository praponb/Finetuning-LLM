from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 5)
    .select("id", "status", "amount", "order_id")
    .orderBy(F.col("id").desc(), F.col("order_id").desc())
)
