from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")

result = (
    df_products
    .select("id", "category", F.count("*").over(w).alias("category_count"))
    .orderBy(F.col("category_count").desc(), F.col("id").desc())
)
