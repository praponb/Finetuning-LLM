from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")

result = (
    df_inventory
    .select("id", "product_id", "last_restock", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
    .orderBy(F.col("row_num").desc())
)
