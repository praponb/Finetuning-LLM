from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price", "id")

result = (
    df_products
    .select("id", "brand", "price", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
    .select("id", "brand", "price")
    .orderBy(F.col("brand").desc())
)
