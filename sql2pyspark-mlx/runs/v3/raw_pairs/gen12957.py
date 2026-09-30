from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")

result = (
    df_products
    .select("id", "brand", "price", F.sum("price").over(w).alias("brand_sum_price"))
    .orderBy(F.col("id").desc())
)
