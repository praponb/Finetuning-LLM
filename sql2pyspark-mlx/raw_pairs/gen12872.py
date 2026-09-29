from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc())

result = (
    df_shipments
    .filter(F.year("shipped_date") == 2023)
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 3)
)
