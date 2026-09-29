from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost").desc(), "id")

result = (
    df_products
    .filter(F.col("cost") >= 660)
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 2)
    .select("id", "category", "cost", "brand")
)
