from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_inventory
    .filter(F.col("product_id") == 5)
    .select(
        "id",
        "product_id",
        "quantity",
        "store_id",
        F.sum("quantity").over(w).alias("product_id_sum_quantity"),
        F.last("store_id").over(w2).alias("last_store_id"),
    )
    .orderBy(F.col("quantity").desc())
)
