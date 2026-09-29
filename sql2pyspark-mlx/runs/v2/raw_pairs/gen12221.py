from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")

result = (
    df_inventory
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "product_id", "last_restock", "store_id")
    .orderBy(F.col("product_id").desc(), "last_restock")
)
