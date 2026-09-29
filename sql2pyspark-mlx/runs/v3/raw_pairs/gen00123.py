from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc())

result = (
    df_products
    .select("id", "brand", "stock_qty", "price", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
)
