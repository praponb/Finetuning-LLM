from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units")

result = (
    df_store_sales
    .select("id", "store_id", "units", "product_id", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
)
