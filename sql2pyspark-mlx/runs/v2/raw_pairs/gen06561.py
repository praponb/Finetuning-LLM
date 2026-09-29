from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "store_id",
        "last_restock",
        "quantity",
        F.lead("quantity", 1).over(w).alias("next_quantity"),
    )
    .orderBy(F.col("id").desc())
)
