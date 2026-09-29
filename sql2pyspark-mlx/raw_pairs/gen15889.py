from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 8]))
    .select(
        "id",
        "product_id",
        "quantity",
        "store_id",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
    .select("id", "product_id", "quantity", "store_id")
    .orderBy(F.col("product_id").desc())
)
