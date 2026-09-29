from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost")

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Kerry", "DHL"]))
    .select("id", "status", "cost", "shipped_date", F.dense_rank().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .select("id", "status", "cost", "shipped_date")
    .orderBy("cost", "shipped_date")
)
