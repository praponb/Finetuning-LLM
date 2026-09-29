from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc())

result = (
    df_store_sales
    .filter(F.col("store_id").isin([4, 5]))
    .select("id", "store_id", "revenue", F.dense_rank().over(w).alias("dense_rnk"))
    .orderBy(F.col("store_id").desc(), "id")
)
