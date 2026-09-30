from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")

result = (
    df_stores
    .withColumn("max_sqft_by_city", F.max("sqft").over(w))
    .filter(F.col("sqft") == F.col("max_sqft_by_city"))
    .select("id", "city", "sqft", "region")
)
