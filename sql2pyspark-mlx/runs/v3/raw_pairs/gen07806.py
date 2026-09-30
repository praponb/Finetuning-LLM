from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units")

result = (
    df_store_sales
    .filter(F.col("store_id") == 3)
    .select("id", "store_id", "units", F.rank().over(w).alias("rnk"))
    .orderBy(F.col("id").desc())
)
