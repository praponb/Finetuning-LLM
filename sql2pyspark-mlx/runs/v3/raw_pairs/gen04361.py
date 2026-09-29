from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date", "id")

result = (
    df_subscriptions
    .filter(F.year("end_date") >= 2023)
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 1)
    .select("id", "plan", "start_date")
    .orderBy(F.col("id").desc())
)
