from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("rating") >= 3.5)
    .select("id", "country", "rating", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 1)
    .select("id", "country", "rating")
    .orderBy(F.col("rating").desc())
)
