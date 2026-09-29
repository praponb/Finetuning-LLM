from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("country") == "SG")
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "country", "rating", "city")
)
