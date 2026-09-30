from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method").orderBy("paid_at", "id")
w2 = Window.partitionBy("status", "order_id").orderBy(F.col("amount").desc_nulls_last())
w3 = Window.partitionBy("method")

result = (
    df_payments
    .withColumn("next_amount", F.lead("amount", 1, 0).over(w))
    .withColumn("amount_cume_dist", F.cume_dist().over(w2))
    .withColumn("method_max_amount", F.max("amount").over(w3))
)
