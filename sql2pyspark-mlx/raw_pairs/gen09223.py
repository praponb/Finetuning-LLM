from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc())

result = (
    df_order_items
    .filter(F.col("unit_price") < 330)
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 3)
)
