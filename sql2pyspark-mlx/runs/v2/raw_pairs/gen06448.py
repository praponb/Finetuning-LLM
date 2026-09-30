from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_inventory
    .select("id", "store_id", "quantity", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
)
