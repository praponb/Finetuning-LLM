from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("product_id").orderBy("last_restock")

result = (
    df_inventory
    .filter(F.year("last_restock") >= 2023)
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity",
        "product_id",
        F.lead("quantity", 1, 0).over(w).alias("next_quantity"),
        F.dense_rank().over(w2).alias("last_restock_dense_rank"),
    )
)
