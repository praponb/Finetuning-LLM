from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("sqft", "id")
w2 = Window.partitionBy("region", "city").orderBy("sqft", "id")
w3 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w4 = Window.partitionBy("city", "region")

result = (
    df_stores
    .withColumn("prev_sqft", F.lag("sqft", 1).over(w_hist))
    .withColumn("sqft_bucket", F.ntile(5).over(w2))
    .withColumn("sqft_bucket_2", F.ntile(4).over(w3))
    .withColumn("city_count_sqft", F.count("sqft").over(w4))
)
