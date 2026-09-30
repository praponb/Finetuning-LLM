from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("rating") < 2.2)
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 5)
    .select("id", "country", "rating")
)
