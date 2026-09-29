from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost")

result = df_shipments.select("id", "cost", F.rank().over(w).alias("cost_rank"))
