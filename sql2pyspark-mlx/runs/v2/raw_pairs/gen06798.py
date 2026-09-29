from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")

result = (
    df_inventory
    .select("id", "store_id", "last_restock", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
    .select("id", "store_id", "last_restock")
    .orderBy(F.col("id").desc())
)
