from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity")

result = (
    df_order_items
    .select("id", "order_id", "quantity", "product_id", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .orderBy(F.col("id").desc())
)
