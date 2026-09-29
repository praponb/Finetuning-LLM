from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 3)
    .orderBy("pos", "paid_at")
)
