from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost")

result = (
    df_shipments
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 2)
)
