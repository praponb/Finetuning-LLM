from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("amount").desc())

result = df_orders.select("id", "channel", "amount", F.rank().over(w).alias("amount_rank"))
