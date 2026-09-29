from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("store_id") == 5)
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity",
        F.lead("quantity", 1).over(w).alias("next_quantity"),
    )
)
