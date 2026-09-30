from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc())

result = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "carrier", "shipped_date")
    .orderBy("carrier")
)
