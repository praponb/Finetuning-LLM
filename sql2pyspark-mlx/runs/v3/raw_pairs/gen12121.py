from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("last_restock").between("2025-06-01", "2025-07-01"))
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        F.lag("quantity", 2).over(w).alias("prev_quantity"),
        F.lag("quantity", 1, 0).over(w).alias("prev_quantity_2"),
    )
)
