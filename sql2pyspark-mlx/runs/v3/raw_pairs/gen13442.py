from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_store_sales
    .filter(F.col("revenue") > 3600)
    .select("id", "product_id", F.count("*").over(w).alias("product_id_count"))
)
