from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc())

result = (
    df_subscriptions
    .filter(~F.col("status").isin(["CANCELLED", "PAUSED", "EXPIRED"]))
    .select("id", "customer_id", "monthly_fee", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 2)
    .select("id", "customer_id", "monthly_fee")
    .orderBy("id", "customer_id")
)
