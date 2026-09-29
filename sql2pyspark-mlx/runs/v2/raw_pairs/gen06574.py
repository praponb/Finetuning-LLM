from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc())

result = (
    df_payments
    .select("id", "order_id", "amount", "status", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
)
