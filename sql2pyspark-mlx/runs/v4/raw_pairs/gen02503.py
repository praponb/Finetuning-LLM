from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("start_date").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 180)
    .select("id", "plan", "start_date", "status", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 5)
    .select("id", "plan", "start_date", "status")
    .orderBy("status")
)
