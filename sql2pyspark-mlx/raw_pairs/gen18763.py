from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc())

result = (
    df_payments
    .select("id", "method", "amount", "order_id", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "method", "amount", "order_id")
    .orderBy(F.col("order_id").desc(), "method")
)
