from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.year("last_restock") == 2025)
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        F.lag("quantity", 2).over(w).alias("prev_quantity"),
    )
)
