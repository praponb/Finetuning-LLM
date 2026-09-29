from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date")

result = (
    df_stores
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 1)
    .select("id", "region", "opened_date", "sqft")
    .orderBy(F.col("id").desc())
)
