from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc())

result = (
    df_stores
    .filter(F.col("region").isin(["South", "East"]))
    .select("id", "region", "sqft", F.rank().over(w).alias("rnk"))
    .orderBy(F.col("sqft").desc())
)
