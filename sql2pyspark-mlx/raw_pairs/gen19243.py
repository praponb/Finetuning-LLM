from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "brand").orderBy(F.col("stock_qty").desc_nulls_last(), "id")

result = (
    df_products
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "product_name", "category", "brand", "cost", "tags", "supplier_id")
)
