from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").desc())

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select("id", "method", "paid_at", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 5)
    .select("id", "method", "paid_at")
    .orderBy("paid_at", F.col("method").desc())
)
