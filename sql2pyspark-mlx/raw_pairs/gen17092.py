from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .filter(F.col("region") == "West")
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "region", "sqft", "store_name")
    .orderBy(F.col("sqft").desc())
)
