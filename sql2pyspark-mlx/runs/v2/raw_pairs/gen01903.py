from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("unit_price").desc())

result = (
    df_order_items
    .filter(F.col("order_id").isin([16, 17]))
    .select("id", "unit_price", F.percent_rank().over(w).alias("unit_price_pct_rank"))
)
