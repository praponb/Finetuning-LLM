from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date")

result = (
    df_stores
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "region", "opened_date")
    .orderBy(F.col("id").desc(), "region")
)
