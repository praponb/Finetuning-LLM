from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("stock_qty", "id")

result = (
    df_products
    .select("id", "brand", "stock_qty", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 2)
    .select("id", "brand", "stock_qty")
)
