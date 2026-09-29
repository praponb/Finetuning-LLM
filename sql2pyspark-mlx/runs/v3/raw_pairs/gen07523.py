from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id")

result = df_shipments.select("id", "delivered_date", F.ntile(10).over(w).alias("quartile"))
