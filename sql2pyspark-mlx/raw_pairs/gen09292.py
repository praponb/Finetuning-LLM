from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc_nulls_first(), "id")

deduped = (
    df_order_items
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = deduped.select("id", "order_id", "product_id", "unit_price")
