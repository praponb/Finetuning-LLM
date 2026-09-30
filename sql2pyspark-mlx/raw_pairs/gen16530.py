from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([2, 7, 11]))
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 2)
    .select("id", "plan", "monthly_fee")
    .orderBy("plan")
)
