from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft")

result = (
    df_stores
    .filter(F.col("region") != "East")
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 3)
    .orderBy("city")
)
