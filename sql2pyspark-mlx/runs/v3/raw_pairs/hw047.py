from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id")

result = df_orders.select("id", "order_date", F.lead("order_date", 1).over(w).alias("next_order_date"))
