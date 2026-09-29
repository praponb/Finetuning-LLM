from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc())

result = (
    df_inventory
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .orderBy("row_num")
)
