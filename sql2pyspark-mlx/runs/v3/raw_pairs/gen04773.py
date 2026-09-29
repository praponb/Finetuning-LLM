from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").desc(), "id")

result = (
    df_products
    .select("id", "brand", "cost", "price", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .select("id", "brand", "cost", "price")
)
