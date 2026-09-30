from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity")

result = (
    df_order_items
    .filter(F.col("order_id").isin([8, 18]))
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") == 1)
    .orderBy(F.col("rnk").desc())
)
