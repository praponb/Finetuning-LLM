from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price").desc())

result = (
    df_products
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") == 1)
)
