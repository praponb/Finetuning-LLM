from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units")

result = (
    df_store_sales
    .filter(F.col("units") <= 37)
    .select("id", "store_id", "units", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 5)
    .select("id", "store_id", "units")
)
