from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc())

result = (
    df_stores
    .filter(~F.col("region").isin(["North", "Central", "South"]))
    .select("id", "region", "sqft", F.dense_rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
)
