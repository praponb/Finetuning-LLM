from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id")
w2 = Window.partitionBy(F.year("last_restock")).orderBy("last_restock", "id")

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "store_id",
        "last_restock",
        "quantity",
        F.lag("quantity", 1, 0).over(w).alias("prev_quantity"),
        F.lead("quantity", 1, 0).over(w2).alias("next_quantity"),
    )
    .orderBy("last_restock")
)
