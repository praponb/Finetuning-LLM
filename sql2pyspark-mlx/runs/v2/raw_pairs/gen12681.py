from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc())

result = (
    df_order_items
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 3)
    .orderBy(F.col("unit_price").desc())
)
