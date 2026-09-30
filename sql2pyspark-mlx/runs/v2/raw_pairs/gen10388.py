from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").desc(), "id")

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "REFUNDED"]))
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 5)
    .select("id", "method", "paid_at", "status")
    .orderBy(F.col("status").desc())
)
