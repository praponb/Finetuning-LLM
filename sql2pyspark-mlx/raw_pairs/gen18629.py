from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").desc())

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([3, 6, 7]))
    .select("id", "plan", "end_date", F.rank().over(w).alias("rnk"))
)
