from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier")

result = df_shipments.select("id", "carrier", F.count("*").over(w).alias("carrier_count"))
