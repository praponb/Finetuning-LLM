from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating")

result = (
    df_suppliers
    .filter(F.col("city") == "Chiang Mai")
    .select("id", "city", "rating", F.dense_rank().over(w).alias("rating_dense_rank"))
)
