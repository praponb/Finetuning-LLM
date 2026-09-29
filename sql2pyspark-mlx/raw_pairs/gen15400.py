from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units", "id")

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 2, 5, 7]))
    .select("id", "store_id", "units", F.ntile(10).over(w).alias("units_ntile"))
)
