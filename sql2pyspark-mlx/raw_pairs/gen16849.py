from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id")

cleaned = (
    df_inventory
    .filter(F.year("last_restock") >= 2023)
    .withColumn("quantity_num", F.col("quantity").cast("string"))
)

result = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
