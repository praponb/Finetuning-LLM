from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.year("last_restock") >= 2023)
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        "store_id",
        F.lead("quantity", 1).over(w).alias("next_quantity"),
        F.lead("quantity", 1).over(w2).alias("next_quantity_2"),
    )
    .orderBy(F.col("id").desc(), F.col("next_quantity_2").desc())
)
