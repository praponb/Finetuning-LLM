from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .select("id", "city", "rating", "country", F.dense_rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "city", "rating", "country")
)
