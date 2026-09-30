from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount")

result = (
    df_payments
    .filter(F.col("amount").isNotNull())
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 2)
    .orderBy("method")
)
