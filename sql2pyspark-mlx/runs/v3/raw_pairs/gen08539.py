from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")

result = (
    df_inventory
    .filter(F.month("last_restock").isin([2, 10, 11]))
    .select("id", "product_id", "last_restock", F.row_number().over(w).alias("row_num"))
)
