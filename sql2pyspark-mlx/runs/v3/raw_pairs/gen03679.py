from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")

result = (
    df_inventory
    .select("id", "store_id", "quantity", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "store_id", "quantity")
    .orderBy(F.col("quantity").desc())
)
