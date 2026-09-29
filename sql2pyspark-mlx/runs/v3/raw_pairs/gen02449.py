from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc())

result = (
    df_subscriptions
    .select("id", "status", "monthly_fee", "customer_id", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 5)
    .orderBy("id")
)
