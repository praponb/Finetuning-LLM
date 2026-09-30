from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("end_date").desc())

result = (
    df_subscriptions
    .filter(F.col("end_date") > "2025-04-01")
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 5)
    .select("id", "customer_id", "end_date", "monthly_fee")
    .orderBy(F.col("customer_id").desc(), F.col("end_date").desc())
)
