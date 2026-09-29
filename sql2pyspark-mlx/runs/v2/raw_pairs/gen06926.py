from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")

result = (
    df_payments
    .select("id", "status", "paid_at", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "status", "paid_at")
    .orderBy("paid_at")
)
