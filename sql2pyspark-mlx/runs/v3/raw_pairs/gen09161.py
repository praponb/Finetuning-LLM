from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units")

result = (
    df_store_sales
    .select("id", "store_id", "units", F.dense_rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .select("id", "store_id", "units")
    .orderBy(F.col("id").desc())
)
