from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("end_date").desc())

result = (
    df_subscriptions
    .filter(F.col("status").isin(["ACTIVE", "EXPIRED", "PAUSED"]))
    .select("id", "status", "end_date", "plan", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
)
