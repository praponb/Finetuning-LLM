from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft")

result = (
    df_stores
    .select("id", "region", "sqft", F.dense_rank().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
    .select("id", "region", "sqft")
    .orderBy("region")
)
