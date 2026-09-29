from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 6]))
    .select("id", "product_id", "sale_date", F.row_number().over(w).alias("row_num"))
    .orderBy("product_id")
)
