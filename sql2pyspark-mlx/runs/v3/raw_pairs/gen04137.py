from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy("amount", "id")

result = (
    df_payments
    .select("id", "status", "order_id", "amount", F.row_number().over(w).alias("rn"))
)
