from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id")
w2 = Window.orderBy("last_restock", "id")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "quantity",
        F.lead("quantity", 1).over(w).alias("next_quantity"),
        F.lead("quantity", 1).over(w2).alias("next_quantity_2"),
    )
)
