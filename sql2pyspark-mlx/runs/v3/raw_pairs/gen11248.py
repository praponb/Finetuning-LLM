from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating")

result = (
    df_suppliers
    .filter(F.col("rating") <= 2.6)
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "country", "rating", "city")
)
