from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Singapore", "Berlin", "Chiang Mai"]))
    .select("id", "city", "rating", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .select("id", "city", "rating")
    .orderBy("rating", "city")
)
