from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")

result = (
    df_order_items
    .filter(F.col("order_id").isin([16, 20]))
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") == 1)
    .orderBy(F.col("product_id").desc())
)
