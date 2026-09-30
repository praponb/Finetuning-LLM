from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("start_date").desc())

result = (
    df_subscriptions
    .select("id", "plan", "start_date", F.rank().over(w).alias("start_date_rank"))
)
