from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft")

result = df_stores.select("id", "city", "sqft", F.rank().over(w).alias("sqft_rank"))
