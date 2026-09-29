from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")

result = (
    df_order_items
    .select("id", "product_id", "unit_price", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .select("id", "product_id", "unit_price")
)
