from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 4, 5]))
    .select(
        "id",
        "store_id",
        "quantity",
        F.min("quantity").over(w).alias("store_id_min_quantity"),
    )
    .orderBy(F.col("store_id").desc())
)
