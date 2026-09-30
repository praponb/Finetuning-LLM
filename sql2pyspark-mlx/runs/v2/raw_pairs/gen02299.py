from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")

result = (
    df_inventory
    .filter(F.month("last_restock").isin([1, 2, 12]))
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 5)
)
