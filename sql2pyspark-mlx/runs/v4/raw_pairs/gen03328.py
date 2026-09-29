from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .filter(F.array_contains("tags", "clearance"))
    .select("id", "category", "price", F.ntile(3).over(w).alias("quartile"))
)
