from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("paid_at").desc(), "id")

latest = (
    df_payments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .select("id", "amount", "paid_at")
    .orderBy(F.col("paid_at").desc())
)
