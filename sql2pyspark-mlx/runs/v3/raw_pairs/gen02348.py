from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount").desc(), "id")

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 22)
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 2)
    .select("id", "channel", "discount")
    .orderBy("channel")
)
