from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-12-03", "2025-02-01"))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "store_id", "last_restock")
    .orderBy(F.col("id").desc(), "store_id")
)
