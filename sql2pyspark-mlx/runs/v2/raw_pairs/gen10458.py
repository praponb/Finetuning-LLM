from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date")

result = (
    df_subscriptions
    .filter(F.col("start_date") > "2023-07-01")
    .select("id", "plan", "end_date", F.rank().over(w).alias("rnk"))
    .orderBy("plan", F.col("id").desc())
)
