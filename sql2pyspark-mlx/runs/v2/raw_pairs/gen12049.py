from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        "store_id",
        F.last("store_id").over(w).alias("last_store_id"),
    )
)
