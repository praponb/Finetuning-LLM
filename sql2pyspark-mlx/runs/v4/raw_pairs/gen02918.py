from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_last())
w2 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_inventory
    .select(
        "*",
        F.rank().over(w).alias("quantity_rank"),
        F.ntile(4).over(w2).alias("quantity_bucket"),
    )
)
