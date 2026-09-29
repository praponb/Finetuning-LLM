from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")

result = (
    df_inventory
    .withColumn("min_quantity_by_store", F.min("quantity").over(w))
    .filter(F.col("quantity") == F.col("min_quantity_by_store"))
    .select("id", "store_id", "quantity", "last_restock")
    .orderBy("quantity")
)
