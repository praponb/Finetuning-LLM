from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy(F.col("price_whole").desc(), "id")

base = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-03-02"))) & F.col("category").isin(["Sports", "Books", "Grocery"]))
    .withColumn("price_whole", F.floor("price"))
)

result = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)
