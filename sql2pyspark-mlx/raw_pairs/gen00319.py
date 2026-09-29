from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_payments
    .withColumn("min_amount_by_status", F.min("amount").over(w))
    .filter(F.col("amount") == F.col("min_amount_by_status"))
    .select("id", "status", "amount", "order_id")
)
