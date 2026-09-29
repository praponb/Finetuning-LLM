from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 4, 5]))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
)
