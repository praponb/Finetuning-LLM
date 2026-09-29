from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units")

result = (
    df_store_sales
    .select("id", "product_id", "units", "revenue", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 2)
    .select("id", "product_id", "units", "revenue")
)
