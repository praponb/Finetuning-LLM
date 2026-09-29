from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 3)
    .orderBy(F.col("store_id").desc(), F.col("channel").desc())
)
