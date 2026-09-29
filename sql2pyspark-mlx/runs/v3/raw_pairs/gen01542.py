from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 2)
    .select("id", "country", "rating")
)
