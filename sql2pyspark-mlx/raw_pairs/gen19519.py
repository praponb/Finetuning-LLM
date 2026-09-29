from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 6]))
    .select("id", "store_id", "quantity", F.row_number().over(w).alias("row_num"))
)
