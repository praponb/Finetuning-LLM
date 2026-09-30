from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("stock_qty")

result = (
    df_products
    .filter(F.col("price").between(1090, 1180))
    .select("id", "stock_qty", F.percent_rank().over(w).alias("stock_qty_pct_rank"))
)
