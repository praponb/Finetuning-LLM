from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.last("last_restock").over(w).alias("last_last_restock"),
    )
    .orderBy("id", F.col("quantity").desc())
)
