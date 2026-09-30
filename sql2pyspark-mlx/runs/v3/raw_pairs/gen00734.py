from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .filter(F.col("order_id") == 30)
    .select("id", "order_id", "status", "amount", F.row_number().over(w).alias("rn"))
)
