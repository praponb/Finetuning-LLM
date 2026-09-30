from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc())

result = (
    df_order_items
    .select("id", "order_id", "quantity", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .orderBy("id")
)
