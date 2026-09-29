from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date")

ranked = (
    df_subscriptions
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .select("id", "customer_id", "start_date", "end_date", "monthly_fee", "rn")
    .orderBy(F.col("start_date").asc_nulls_last())
)
