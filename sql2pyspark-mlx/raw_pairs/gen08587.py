from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost")

result = (
    df_shipments
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") == 1)
    .orderBy(F.col("rnk").desc())
)
