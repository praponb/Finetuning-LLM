from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount", "id")

result = df_orders.select("id", "channel", "discount", F.ntile(3).over(w).alias("bucket"))
