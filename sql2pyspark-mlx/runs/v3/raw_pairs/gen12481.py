from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc(), "id")

result = (
    df_payments
    .filter(F.to_date("paid_at") < "2023-05-01")
    .select("id", "order_id", "paid_at", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .orderBy(F.col("id").desc(), "paid_at")
)
