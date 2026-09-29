from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc())

result = (
    df_payments
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "order_id", "paid_at")
    .orderBy(F.col("order_id").desc(), "id")
)
