from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price")

result = (
    df_order_items
    .filter(F.col("quantity").isNotNull())
    .select("id", "order_id", "unit_price", F.dense_rank().over(w).alias("dense_rnk"))
)
