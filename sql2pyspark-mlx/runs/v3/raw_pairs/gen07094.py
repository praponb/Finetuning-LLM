from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy(F.col("stock_qty").desc())

result = (
    df_products
    .filter(F.upper("product_name").like("%SB-%"))
    .select("id", "category", "brand", "stock_qty", F.rank().over(w).alias("rnk"))
    .orderBy("brand")
)
