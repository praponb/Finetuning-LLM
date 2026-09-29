from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 1)
    .select("id", "brand", "price")
    .orderBy(F.col("id").desc())
)
