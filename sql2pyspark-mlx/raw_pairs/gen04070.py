from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft")

result = (
    df_stores
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 1)
)
