from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc())

result = (
    df_orders
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 2)
)
