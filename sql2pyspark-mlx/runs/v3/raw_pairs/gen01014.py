from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.dense_rank().over(w).alias("quantity_dense_rank"),
    )
)
