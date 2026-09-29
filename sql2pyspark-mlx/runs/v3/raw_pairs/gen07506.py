from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price")

result = (
    df_products
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 3)
)
