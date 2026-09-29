from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc(), "id")

result = (
    df_payments
    .select("id", "status", "paid_at", "order_id", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "status", "paid_at", "order_id")
)
