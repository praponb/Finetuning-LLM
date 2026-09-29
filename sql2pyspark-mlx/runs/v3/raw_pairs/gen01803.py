from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 6]))
    .select(
        "id",
        "product_id",
        "quantity",
        "store_id",
        "last_restock",
        F.sum("quantity").over(w).alias("product_id_sum_quantity"),
        F.lead("quantity", 1).over(w2).alias("next_quantity"),
    )
)
