from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")

result = (
    df_stores
    .withColumn("min_sqft_by_city", F.min("sqft").over(w))
    .filter(F.col("sqft") == F.col("min_sqft_by_city"))
    .select("id", "city", "sqft")
    .orderBy(F.col("id").desc())
)
