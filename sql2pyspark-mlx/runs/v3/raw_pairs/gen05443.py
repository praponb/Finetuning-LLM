from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount")

result = df_orders.select("id", "amount", F.rank().over(w).alias("rnk"))
