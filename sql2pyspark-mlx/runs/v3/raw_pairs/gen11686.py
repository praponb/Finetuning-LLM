from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price")

result = (
    df_order_items
    .select("id", "order_id", "unit_price", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .orderBy(F.col("order_id").desc(), F.col("id").desc())
)
