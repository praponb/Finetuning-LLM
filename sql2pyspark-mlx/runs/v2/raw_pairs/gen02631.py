from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("stock_qty", "id")

result = (
    df_products
    .select("id", "brand", "stock_qty", "category", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
)
