from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")

result = (
    df_products
    .filter(F.col("brand") != "Acme")
    .select("id", "category", F.count("*").over(w).alias("category_count"))
)
