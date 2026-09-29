from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .select("id", "store_id", "quantity", "product_id", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 1)
    .orderBy(F.col("rn").desc())
)
