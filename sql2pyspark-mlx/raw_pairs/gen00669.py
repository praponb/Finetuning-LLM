from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("discount").desc())

result = (
    df_orders
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 5)
    .select("id", "status", "discount", "channel")
)
