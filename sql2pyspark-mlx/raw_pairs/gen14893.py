from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")

result = (
    df_inventory
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 1)
    .select("id", "store_id", "last_restock", "quantity")
    .orderBy(F.col("id").desc())
)
