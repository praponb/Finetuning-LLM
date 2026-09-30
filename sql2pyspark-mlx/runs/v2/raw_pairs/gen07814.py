from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .filter(F.col("quantity").between(6, 7))
    .select("id", "product_id", "quantity", F.ntile(3).over(w).alias("quantity_ntile"))
)
