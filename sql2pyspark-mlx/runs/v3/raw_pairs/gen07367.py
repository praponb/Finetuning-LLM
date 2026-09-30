from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 4, 5]))
    .select(
        "id",
        "last_restock",
        "quantity",
        F.lead("quantity", 1).over(w).alias("next_quantity"),
    )
)
