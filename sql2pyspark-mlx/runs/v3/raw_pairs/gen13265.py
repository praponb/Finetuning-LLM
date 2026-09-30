from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc())

result = (
    df_inventory
    .filter(F.col("product_id") == 5)
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 5)
    .orderBy("row_num")
)
