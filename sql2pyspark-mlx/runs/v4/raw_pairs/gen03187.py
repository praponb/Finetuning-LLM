from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost").desc())

result = (
    df_products
    .filter(F.col("category").isin(["Books", "Sports", "Electronics"]))
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "category", "cost")
    .orderBy("category", F.col("id").desc())
)
