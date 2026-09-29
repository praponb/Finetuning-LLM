from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .filter(F.col("channel").isin(["partner", "store"]))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") == 1)
    .orderBy(F.col("amount").desc())
)
