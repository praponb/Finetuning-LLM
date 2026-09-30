from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft")

result = (
    df_stores
    .filter(F.col("sqft") != 8087)
    .select("id", "region", "sqft", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 2)
)
