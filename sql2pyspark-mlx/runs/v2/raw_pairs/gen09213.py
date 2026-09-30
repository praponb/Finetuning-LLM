from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")

result = df_stores.select("id", "region", F.count("*").over(w).alias("region_count"))
