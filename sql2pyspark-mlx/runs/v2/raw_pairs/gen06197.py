from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc())

result = (
    df_stores
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "city", "sqft")
)
