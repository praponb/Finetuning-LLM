from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 4, 7]))
    .select(
        "id",
        "product_id",
        "quantity",
        "store_id",
        F.last("store_id").over(w).alias("last_store_id"),
        F.first("store_id").over(w2).alias("first_store_id"),
    )
)
