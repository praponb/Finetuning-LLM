from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost").desc(), "id")

result = (
    df_products
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 5)
)
