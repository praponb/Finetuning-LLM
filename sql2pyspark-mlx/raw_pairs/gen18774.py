from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft")

result = (
    df_stores
    .filter(F.lower(F.trim("store_name")) == "mall central")
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 1)
)
