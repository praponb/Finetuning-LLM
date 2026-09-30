from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 5)
    .orderBy(F.col("shipping_fee").desc())
)
