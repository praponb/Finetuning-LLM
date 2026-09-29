from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("quantity") < 3)
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 1)
)
