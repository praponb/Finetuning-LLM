from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc(), "id")

result = (
    df_payments
    .filter(F.col("amount") < 1380)
    .select("id", "order_id", "paid_at", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
)
