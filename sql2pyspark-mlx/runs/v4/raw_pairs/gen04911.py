from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").desc(), "id")

result = (
    df_payments
    .select("id", "method", "paid_at", "amount", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .orderBy("amount")
)
