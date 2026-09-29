from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity")

result = df_order_items.select("id", "quantity", F.rank().over(w).alias("rnk"))
