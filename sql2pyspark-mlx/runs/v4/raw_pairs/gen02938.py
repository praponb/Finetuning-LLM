from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select("id", "order_id", "paid_at", F.row_number().over(w).alias("row_num"))
)
