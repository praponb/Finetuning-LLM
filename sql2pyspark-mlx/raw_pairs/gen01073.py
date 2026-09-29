from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id")

result = (
    df_stores
    .filter(F.col("city").isin(["New York", "London"]))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") == 1)
    .orderBy(F.col("sqft").desc())
)
