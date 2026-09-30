from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")

result = (
    df_subscriptions
    .withColumn("min_monthly_fee_by_plan", F.min("monthly_fee").over(w))
    .filter(F.col("monthly_fee") == F.col("min_monthly_fee_by_plan"))
    .select("id", "plan", "monthly_fee", "end_date")
)
