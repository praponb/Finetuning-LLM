from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc())

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Kerry", "DHL", "UPS"]))
    .select("id", "carrier", "delivered_date", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 1)
    .select("id", "carrier", "delivered_date")
    .orderBy(F.col("id").desc())
)
