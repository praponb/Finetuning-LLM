from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date")

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 1)
    .select("id", "customer_id", "start_date", "monthly_fee")
    .orderBy("id")
)
