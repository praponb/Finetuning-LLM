from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "quantity",
        F.max("quantity").over(w).alias("store_id_max_quantity"),
    )
    .orderBy(F.col("store_id").desc(), F.col("quantity").desc())
)
