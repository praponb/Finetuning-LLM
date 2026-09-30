from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc())

result = (
    df_shipments
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 1)
    .select("id", "status", "cost", "carrier")
)
