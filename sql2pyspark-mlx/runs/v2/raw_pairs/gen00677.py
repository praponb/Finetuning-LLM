from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue")

result = (
    df_store_sales
    .filter(F.col("product_id").isin([7, 8]))
    .select("id", "store_id", "revenue", F.rank().over(w).alias("rnk"))
    .orderBy(F.col("id").desc())
)
