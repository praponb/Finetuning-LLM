from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")

result = (
    df_order_items
    .filter(F.col("product_id") == 13)
    .select("id", "order_id", "unit_price", F.row_number().over(w).alias("rn"))
)
