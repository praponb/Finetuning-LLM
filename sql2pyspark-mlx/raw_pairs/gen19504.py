from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("launch_date").desc())

result = (
    df_products
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "category", "launch_date")
    .orderBy(F.col("category").desc(), "launch_date")
)
