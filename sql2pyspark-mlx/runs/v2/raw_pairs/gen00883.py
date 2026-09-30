from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("launch_date").desc(), "id")

result = (
    df_products
    .filter(F.col("category").isin(["Books", "Sports"]))
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 2)
    .orderBy("brand", F.col("price").desc())
)
