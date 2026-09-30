from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-07-01", "2024-10-01"))
    .select(
        "id",
        "product_id",
        "quantity",
        F.sum("quantity").over(w).alias("product_id_sum_quantity"),
    )
)
