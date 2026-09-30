from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")

result = (
    df_store_sales
    .filter(F.col("product_id") == 1)
    .select("id", "product_id", "revenue", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 1)
    .select("id", "product_id", "revenue")
)
