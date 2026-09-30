from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc())

result = (
    df_stores
    .filter(F.col("store_name").isNull())
    .select("id", "region", "sqft", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 2)
)
