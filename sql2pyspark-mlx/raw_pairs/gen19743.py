from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 4, 6]))
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 5)
    .select("id", "product_id", "quantity", "last_restock")
)
