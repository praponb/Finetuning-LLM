from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("price")

result = (
    df_products
    .select("id", "category", "price", "stock_qty", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
)
