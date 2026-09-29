from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 2]))
    .select(
        "id",
        "last_restock",
        "quantity",
        F.lag("quantity", 1).over(w).alias("prev_quantity"),
    )
)
