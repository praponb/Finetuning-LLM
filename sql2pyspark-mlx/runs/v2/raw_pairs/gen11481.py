from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost")

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([5, 7, 11]))
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 3)
)
