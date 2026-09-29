from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date")

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 5)
    .select("id", "customer_id", "end_date", "start_date")
)
