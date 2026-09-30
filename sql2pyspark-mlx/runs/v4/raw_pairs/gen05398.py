from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc(), "id")

result = df_stores.select("id", "region", "city", "sqft", F.row_number().over(w).alias("rn"))
