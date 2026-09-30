from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_order_items
    .filter(F.col("unit_price") >= 260)
    .select("id", "product_id", "quantity", F.ntile(4).over(w).alias("quartile"))
)
