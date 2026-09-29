from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date")

result = (
    df_stores
    .filter(F.col("region") == "North")
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") == 1)
)
