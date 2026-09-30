from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("end_date").desc())

result = (
    df_subscriptions
    .select("id", "status", "end_date", "start_date", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 2)
    .orderBy("start_date", "id")
)
