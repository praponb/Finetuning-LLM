from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("greatest_unit_price_quantity").desc_nulls_first(), "id")

cleaned = (
    df_order_items
    .filter(F.col("order_id").isin([5, 23]))
    .withColumn("greatest_unit_price_quantity", F.greatest("unit_price", "quantity"))
)

result = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "order_id", "quantity", "unit_price", "greatest_unit_price_quantity")
)
