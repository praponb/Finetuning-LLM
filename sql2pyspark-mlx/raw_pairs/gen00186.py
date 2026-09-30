from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc())

result = (
    df_payments
    .filter(F.col("method").isNull())
    .select("id", "status", "paid_at", "order_id", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "status", "paid_at", "order_id")
)
