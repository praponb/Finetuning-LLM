from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 1)
    .select("id", "order_id", "quantity", "unit_price")
    .orderBy(F.col("quantity").desc(), "order_id")
)
