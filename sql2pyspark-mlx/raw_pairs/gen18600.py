from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("paid_at"), "status").orderBy("paid_at", "id")

result = (
    df_payments
    .select("id", "paid_at", "status", F.row_number().over(w).alias("paid_at_row"))
    .orderBy(F.col("id").desc())
)
