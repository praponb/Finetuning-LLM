from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")

result = (
    df_inventory
    .filter(F.col("last_restock") <= "2025-05-21")
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "store_id", "last_restock")
)
