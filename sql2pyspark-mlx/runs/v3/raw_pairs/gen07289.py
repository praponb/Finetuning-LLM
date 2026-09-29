from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date")

result = (
    df_store_sales
    .filter(F.col("revenue").between(1100, 2300))
    .select("id", "product_id", "sale_date", F.rank().over(w).alias("sale_date_rank"))
)
