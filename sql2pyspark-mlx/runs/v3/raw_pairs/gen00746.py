from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .filter(F.col("paid_at") < "2024-11-01")
    .select("id", "order_id", "amount", F.row_number().over(w).alias("rn"))
)
