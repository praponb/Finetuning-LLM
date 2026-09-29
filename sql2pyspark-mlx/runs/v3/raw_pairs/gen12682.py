from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("store_id") == 5)
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        F.lead("quantity", 2).over(w).alias("next_quantity"),
    )
)
