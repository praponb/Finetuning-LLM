from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())
w2 = Window.partitionBy("product_id")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "quantity",
        "product_id",
        F.percent_rank().over(w).alias("quantity_pct_rank"),
        F.max("quantity").over(w2).alias("product_id_max_quantity"),
    )
    .orderBy(F.col("store_id").desc())
)
