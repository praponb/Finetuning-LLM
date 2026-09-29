from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([4, 6, 12]))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 5)
    .select("id", "status", "cost")
    .orderBy(F.col("cost").desc())
)
