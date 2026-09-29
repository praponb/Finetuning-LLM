from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").desc())

result = (
    df_customers
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 3)
    .orderBy("city")
)
