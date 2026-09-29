from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")

result = (
    df_payments
    .select("id", "method", "paid_at", "order_id", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
)
