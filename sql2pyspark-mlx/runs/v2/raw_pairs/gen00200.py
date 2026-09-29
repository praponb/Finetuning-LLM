from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 4, 7, 8]))
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        (F.col("quantity") - F.lag("quantity", 2).over(w)).alias("quantity_change"),
    )
)
