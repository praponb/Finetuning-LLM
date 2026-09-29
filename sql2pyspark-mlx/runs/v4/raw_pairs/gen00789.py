from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("unit_price").desc())

result = (
    df_order_items
    .filter(F.col("product_id").isin([20, 29]))
    .select("id", "unit_price", F.rank().over(w).alias("unit_price_rank"))
    .orderBy(F.col("unit_price").desc())
)
