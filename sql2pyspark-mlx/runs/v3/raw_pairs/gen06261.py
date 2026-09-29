from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")

result = (
    df_payments
    .select("id", "status", "paid_at", "order_id", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 5)
)
