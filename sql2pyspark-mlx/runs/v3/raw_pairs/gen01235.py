from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price", "id")

result = (
    df_products
    .select("id", "brand", "price", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
)
