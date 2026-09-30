from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") == 1)
    .orderBy(F.col("order_id").desc())
)
