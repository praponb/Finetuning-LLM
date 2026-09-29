from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft")

result = (
    df_stores
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 5)
    .select("id", "city", "sqft", "region")
    .orderBy("city")
)
