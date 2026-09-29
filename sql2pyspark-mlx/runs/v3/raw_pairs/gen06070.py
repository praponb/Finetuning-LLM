from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 4, 6]))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 3)
)
