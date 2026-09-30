from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc())

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 5, 6]))
    .select("id", "product_id", "units", "revenue", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 1)
    .select("id", "product_id", "units", "revenue")
    .orderBy("product_id", F.col("id").desc())
)
