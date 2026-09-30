from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .filter(F.col("category") == "Fashion")
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") == 1)
)
