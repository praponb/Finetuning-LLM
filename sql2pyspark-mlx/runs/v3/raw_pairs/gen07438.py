from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-08-01", "2024-02-27"))
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        F.sum("quantity").over(w).alias("running_quantity"),
    )
    .orderBy(F.col("id").desc())
)
