from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-03-01", "2023-11-25"))
    .select(
        "id",
        "product_id",
        "last_restock",
        "store_id",
        "quantity",
        F.ntile(10).over(w).alias("last_restock_ntile"),
        F.sum("quantity").over(w2).alias("running_quantity"),
    )
)
