from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("stock_qty")

result = (
    df_products
    .filter(F.col("price") >= 700)
    .select("id", "supplier_id", "stock_qty", F.rank().over(w).alias("stock_qty_rank"))
)
