from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "store_id").orderBy(F.col("discount").desc_nulls_last(), "id")

valid = (
    df_orders
    .filter(F.col("channel").isNotNull())
    .select("*", F.concat("status", F.lit(" - "), "channel").alias("status_channel"))
)

result = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)
