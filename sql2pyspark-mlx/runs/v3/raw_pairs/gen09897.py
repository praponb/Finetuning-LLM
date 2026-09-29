from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        F.lag("quantity", 1).over(w).alias("prev_quantity"),
    )
    .orderBy("quantity")
)
