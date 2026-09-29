from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc())
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("quantity").isNotNull())
    .select(
        "id",
        "store_id",
        "last_restock",
        "product_id",
        "quantity",
        F.dense_rank().over(w).alias("last_restock_dense_rank"),
        F.lag("quantity", 1).over(w2).alias("prev_quantity"),
    )
)
