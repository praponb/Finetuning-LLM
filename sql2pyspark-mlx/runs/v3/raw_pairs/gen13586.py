from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating")

result = df_suppliers.select("id", "country", "rating", F.rank().over(w).alias("rnk"))
