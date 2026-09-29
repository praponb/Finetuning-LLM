from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_order_items
    .filter(F.col("quantity") == 7)
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 1)
)
