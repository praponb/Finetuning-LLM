from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")

result = (
    df_inventory
    .filter(F.col("last_restock") < "2025-03-01")
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 1)
    .select("id", "product_id", "last_restock", "store_id")
    .orderBy("id")
)
