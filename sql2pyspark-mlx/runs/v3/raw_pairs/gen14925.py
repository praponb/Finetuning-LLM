from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .filter(F.col("sqft") == 14819)
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 2)
    .select("id", "region", "sqft")
    .orderBy(F.col("sqft").desc())
)
