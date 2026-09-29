from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc())

result = (
    df_store_sales
    .filter(F.col("revenue") >= 2100)
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)
