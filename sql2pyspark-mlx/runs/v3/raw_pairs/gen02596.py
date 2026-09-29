from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc(), "id")

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "FAILED"]))
    .select("id", "status", "paid_at", "amount", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
)
