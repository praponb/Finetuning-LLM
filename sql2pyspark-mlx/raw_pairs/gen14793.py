from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")

result = (
    df_inventory
    .filter(F.col("product_id").isin([7, 8]))
    .select("id", "store_id", "quantity", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
    .select("id", "store_id", "quantity")
)
