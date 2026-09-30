from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty")

result = (
    df_products
    .filter(F.col("cost").between(350, 550))
    .select("id", "category", "stock_qty", F.rank().over(w).alias("rnk"))
    .orderBy(F.col("id").desc())
)
