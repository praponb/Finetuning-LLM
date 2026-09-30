from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost")

result = df_shipments.select("id", "order_id", "cost", F.rank().over(w).alias("rnk"))
