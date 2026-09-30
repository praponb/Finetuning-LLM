from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 5]))
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        F.first("quantity").over(w).alias("first_quantity"),
    )
)
