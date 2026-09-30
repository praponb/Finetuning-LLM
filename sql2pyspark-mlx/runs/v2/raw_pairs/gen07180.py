from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id")

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 4, 6]))
    .select(
        "id",
        "product_id",
        "store_id",
        "quantity",
        F.max("quantity").over(w).alias("product_id_max_quantity"),
    )
)
