from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .select("id", "category", "price", "brand", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 2)
    .orderBy("brand")
)
