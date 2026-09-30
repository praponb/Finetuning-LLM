from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date")

result = (
    df_store_sales
    .filter(F.col("revenue") < 3400)
    .select("id", "product_id", "sale_date", F.dense_rank().over(w).alias("dense_rnk"))
)
