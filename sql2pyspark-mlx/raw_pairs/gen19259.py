from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating")

result = (
    df_suppliers
    .filter(F.col("rating") <= 3.3)
    .select("id", "country", "city", "rating", F.rank().over(w).alias("rating_rank"))
    .orderBy("rating")
)
