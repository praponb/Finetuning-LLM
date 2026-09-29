from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").desc(), "id")

result = (
    df_payments
    .filter(F.col("paid_at") >= "2023-03-15")
    .select("id", "method", "paid_at", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
    .select("id", "method", "paid_at")
    .orderBy("id")
)
