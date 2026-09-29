from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc(), "id")

result = (
    df_payments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "status", "paid_at")
    .orderBy(F.col("id").desc())
)
