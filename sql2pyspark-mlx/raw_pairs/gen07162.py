from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price", "id")

result = (
    df_products
    .filter(F.col("stock_qty") == 332)
    .select("id", "brand", "price", "category", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
)
