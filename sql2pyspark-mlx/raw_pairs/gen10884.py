from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.month("last_restock").isin([2, 3, 12]))
    .select(
        "id",
        "product_id",
        "quantity",
        "store_id",
        "last_restock",
        F.min("quantity").over(w).alias("product_id_min_quantity"),
        F.lead("quantity", 1, 0).over(w2).alias("next_quantity"),
    )
)
