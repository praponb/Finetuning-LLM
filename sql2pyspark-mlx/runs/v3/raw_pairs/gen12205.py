from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date")

result = (
    df_products
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 2)
)
