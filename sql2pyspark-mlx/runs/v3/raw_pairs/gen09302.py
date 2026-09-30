from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 6]))
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity",
        (F.col("quantity") - F.lag("quantity", 1).over(w)).alias("quantity_change"),
        (F.col("quantity") - F.lag("quantity", 1, 0).over(w)).alias("quantity_change_2"),
    )
)
