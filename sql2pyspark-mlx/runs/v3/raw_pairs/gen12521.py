from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_payments
    .withColumn("max_amount_by_status", F.max("amount").over(w))
    .filter(F.col("amount") == F.col("max_amount_by_status"))
    .select("id", "status", "amount", "method")
    .orderBy(F.col("amount").desc())
)
