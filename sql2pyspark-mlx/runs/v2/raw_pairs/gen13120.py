from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "quantity",
        F.sum("quantity").over(w).alias("store_id_sum_quantity"),
    )
)
