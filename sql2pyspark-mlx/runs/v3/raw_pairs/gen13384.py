from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc())

result = (
    df_payments
    .select("id", "order_id", "paid_at", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .orderBy("id", F.col("order_id").desc())
)
