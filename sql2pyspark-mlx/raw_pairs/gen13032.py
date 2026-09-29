from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)
