from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc())

result = (
    df_shipments
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "carrier", "delivered_date", "cost")
)
