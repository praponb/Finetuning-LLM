from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date", "id")

result = (
    df_subscriptions
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "plan", "start_date", "monthly_fee")
)
