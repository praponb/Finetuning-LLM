from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id")

result = (
    df_stores
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "region", "sqft")
    .orderBy(F.col("id").desc())
)
