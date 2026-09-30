from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc())

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["Kerry", "Flash"]))
    .select("id", "carrier", "delivered_date", F.dense_rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
)
