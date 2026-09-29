from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc_nulls_last(), "id")

latest = (
    df_inventory
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select("id", "store_id", "product_id", "quantity", "last_restock", "row_num")
    .orderBy(F.col("last_restock").desc_nulls_last(), F.col("id").desc())
)
