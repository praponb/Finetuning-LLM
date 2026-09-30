from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft")

result = (
    df_stores
    .filter(F.col("sqft") > 9204)
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 2)
    .select("id", "region", "sqft")
    .orderBy(F.col("sqft").desc())
)
