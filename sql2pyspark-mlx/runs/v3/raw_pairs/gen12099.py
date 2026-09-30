from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_date")

result = df_shipments.select("id", "shipped_date", F.rank().over(w).alias("rnk"))
