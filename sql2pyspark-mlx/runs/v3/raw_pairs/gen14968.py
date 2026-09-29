from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")

result = df_customers.select("id", "city", "age", F.max("age").over(w).alias("city_max_age"))
