from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        F.count("*").over(w).alias("store_id_count"),
    )
)
