from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([1, 5, 11]))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "plan", "monthly_fee", "end_date")
    .orderBy(F.col("id").desc(), "plan")
)
