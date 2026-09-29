from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 4, 5]))
    .select("id", "product_id", "revenue", F.ntile(2).over(w).alias("revenue_ntile"))
)
