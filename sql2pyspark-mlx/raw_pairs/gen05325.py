from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("launch_date")).orderBy("launch_date")

result = df_products.select("id", "launch_date", F.rank().over(w).alias("rnk"))
