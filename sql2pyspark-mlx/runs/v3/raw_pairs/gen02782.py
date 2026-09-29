from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("last_restock").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        "store_id",
        "quantity",
        F.rank().over(w).alias("last_restock_rank"),
        F.last("quantity").over(w2).alias("last_quantity"),
    )
)
