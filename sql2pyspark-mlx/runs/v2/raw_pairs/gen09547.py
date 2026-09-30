from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft")

result = (
    df_stores
    .filter(F.col("region") == "Central")
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 3)
    .orderBy("store_name")
)
