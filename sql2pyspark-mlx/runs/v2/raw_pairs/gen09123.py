from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount")

result = (
    df_payments
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 2)
    .select("id", "order_id", "amount")
    .orderBy(F.col("id").desc())
)
