from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .select("id", "country", "rating", "city", F.dense_rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 2)
    .select("id", "country", "rating", "city")
    .orderBy("country")
)
