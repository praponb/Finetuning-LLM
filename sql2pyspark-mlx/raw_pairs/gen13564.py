from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price")

result = (
    df_order_items
    .select("id", "unit_price", F.dense_rank().over(w).alias("unit_price_dense_rank"))
)
