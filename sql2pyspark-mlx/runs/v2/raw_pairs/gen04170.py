from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("start_date").desc())

result = (
    df_subscriptions
    .filter(F.col("end_date") > "2025-04-01")
    .select("id", "plan", "start_date", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "plan", "start_date")
    .orderBy(F.col("start_date").desc())
)
