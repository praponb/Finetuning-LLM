from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc_nulls_first())

result = (
    df_products
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)
