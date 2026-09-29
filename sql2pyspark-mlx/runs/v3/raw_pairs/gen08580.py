from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy(F.col("end_date").desc())

result = (
    df_subscriptions
    .select("id", "plan", "status", "end_date", F.rank().over(w).alias("end_date_rank"))
)
