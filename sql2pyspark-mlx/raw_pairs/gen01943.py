from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id")
w3 = Window.orderBy("sqft", "id")
w4 = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc(), "id")

with_windows = (
    df_stores
    .filter(F.col("city") == "Tokyo")
    .withColumn("region_sum_sqft", F.sum("sqft").over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("sqft_bucket", F.ntile(3).over(w3))
    .withColumn("sqft_bucket_2", F.ntile(4).over(w4))
)

result = (
    with_windows
    .select("id", "city", "opened_date", "seq_no", "sqft_bucket", "sqft_bucket_2")
)
