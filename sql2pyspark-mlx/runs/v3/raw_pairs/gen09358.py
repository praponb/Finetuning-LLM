from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity")

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-12-25", "2025-04-08"))
    .select("id", "store_id", "quantity", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
)
