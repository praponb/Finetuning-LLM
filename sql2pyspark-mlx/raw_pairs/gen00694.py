from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc())

result = (
    df_stores
    .filter(~F.col("store_name").like("%Expre%"))
    .select("id", "region", "sqft", "city", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
    .select("id", "region", "sqft", "city")
)
