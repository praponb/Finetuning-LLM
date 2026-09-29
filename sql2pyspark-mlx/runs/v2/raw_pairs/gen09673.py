from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc(), "id")

result = (
    df_payments
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 2)
    .select("id", "status", "paid_at")
)
