from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("start_date").desc(), "id")

result = (
    df_subscriptions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 5)
    .select("id", "plan", "start_date", "monthly_fee")
    .orderBy(F.col("id").desc(), "start_date")
)
