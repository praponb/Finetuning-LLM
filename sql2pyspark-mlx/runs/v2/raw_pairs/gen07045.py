from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at")

result = (
    df_payments
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 1)
    .select("id", "order_id", "paid_at", "method")
)
