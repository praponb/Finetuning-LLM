from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc())

result = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select("id", "brand", "stock_qty", F.dense_rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 2)
    .select("id", "brand", "stock_qty")
)
