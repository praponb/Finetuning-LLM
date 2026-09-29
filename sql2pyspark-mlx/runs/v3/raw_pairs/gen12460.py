from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc(), "id")

result = df_orders.select("id", "amount", F.ntile(3).over(w).alias("amount_ntile"))
