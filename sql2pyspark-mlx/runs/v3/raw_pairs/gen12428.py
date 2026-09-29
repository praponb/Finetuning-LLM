from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_inventory
    .withColumn("min_quantity_by_product", F.min("quantity").over(w))
    .filter(F.col("quantity") == F.col("min_quantity_by_product"))
    .select("id", "product_id", "quantity", "store_id")
)
