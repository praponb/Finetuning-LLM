from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region", "city")
w3 = Window.orderBy("sqft", "id")

result = (
    df_stores
    .withColumn("running_sqft", F.sum("sqft").over(w))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4))
    .withColumn("sqft_bucket", F.ntile(3).over(w3))
    .withColumn("sqft_share_2", F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4))
)
