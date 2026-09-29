from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc())

result = (
    df_shipments
    .select("id", "carrier", "cost", "order_id", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "carrier", "cost", "order_id")
    .orderBy(F.col("id").desc())
)
