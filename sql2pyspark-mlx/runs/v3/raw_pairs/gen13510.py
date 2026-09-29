from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date")

result = (
    df_subscriptions
    .filter(~F.col("status").isin(["PAUSED", "EXPIRED"]))
    .select("id", "plan", "start_date", F.rank().over(w).alias("start_date_rank"))
    .orderBy(F.col("id").desc())
)
