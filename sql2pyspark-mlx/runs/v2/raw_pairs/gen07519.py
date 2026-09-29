from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")

result = (
    df_inventory
    .filter(F.col("last_restock") >= "2024-12-01")
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 2)
    .select("id", "store_id", "quantity", "last_restock")
)
