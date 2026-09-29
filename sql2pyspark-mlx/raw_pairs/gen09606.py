from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_inventory
    .filter(F.col("quantity") != 233)
    .select("id", "product_id", "quantity", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .orderBy("product_id", "id")
)
