from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("start_date").desc())

result = df_subscriptions.select("id", "status", "start_date", F.rank().over(w).alias("rnk"))
