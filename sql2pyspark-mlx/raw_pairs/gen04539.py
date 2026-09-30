from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date")

result = (
    df_shipments
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 3)
)
