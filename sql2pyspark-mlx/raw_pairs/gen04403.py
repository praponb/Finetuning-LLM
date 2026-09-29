from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc_nulls_last(), "id")

ranked = (
    df_payments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .select("id", "method", "paid_at", "status")
    .orderBy(F.col("method").desc_nulls_last())
)
