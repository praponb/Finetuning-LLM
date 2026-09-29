from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty")

result = (
    df_products
    .select("id", "category", "stock_qty", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .select("id", "category", "stock_qty")
)
