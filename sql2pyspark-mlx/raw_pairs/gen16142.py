from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("last_restock") >= "2023-12-01")
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        F.lead("quantity", 1).over(w).alias("next_quantity"),
    )
)
