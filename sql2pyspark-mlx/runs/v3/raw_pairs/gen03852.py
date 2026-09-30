from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("discount").desc(), "id")

result = df_orders.select("id", "store_id", "discount", F.ntile(10).over(w).alias("bucket"))
