from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date")

result = (
    df_subscriptions
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "plan", "end_date")
    .orderBy("plan")
)
