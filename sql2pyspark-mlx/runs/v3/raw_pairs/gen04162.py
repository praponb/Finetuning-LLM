from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date")

result = (
    df_products
    .select("id", "category", "launch_date", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "category", "launch_date")
    .orderBy("category", F.col("id").desc())
)
