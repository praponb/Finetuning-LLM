from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc())

result = (
    df_stores
    .select("id", "region", "sqft", "store_name", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 1)
    .select("id", "region", "sqft", "store_name")
)
