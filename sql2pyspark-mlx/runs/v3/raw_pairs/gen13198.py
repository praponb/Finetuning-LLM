from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .select("id", "order_id", "amount", "status", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 1)
    .select("id", "order_id", "amount", "status")
    .orderBy("status")
)
