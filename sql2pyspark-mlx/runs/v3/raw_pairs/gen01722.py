from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc())

result = (
    df_order_items
    .filter(F.col("quantity").between(6, 7))
    .select("id", "product_id", "unit_price", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "product_id", "unit_price")
)
