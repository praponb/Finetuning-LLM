from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "quantity",
        "product_id",
        F.min("quantity").over(w).alias("store_id_min_quantity"),
        F.percent_rank().over(w2).alias("quantity_pct_rank"),
    )
)
