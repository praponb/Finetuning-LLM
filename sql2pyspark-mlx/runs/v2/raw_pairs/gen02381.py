from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock", "id")
w2 = Window.partitionBy("store_id")

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4]))
    .select(
        "id",
        "last_restock",
        "quantity",
        "store_id",
        F.lag("quantity", 1).over(w).alias("prev_quantity"),
        F.max("quantity").over(w2).alias("store_id_max_quantity"),
    )
    .orderBy(F.col("store_id").desc())
)
