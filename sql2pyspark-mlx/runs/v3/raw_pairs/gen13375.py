from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_inventory
    .filter(F.month("last_restock").isin([1, 4, 9]))
    .select(
        "id",
        "product_id",
        "quantity",
        "store_id",
        F.sum("quantity").over(w).alias("product_id_sum_quantity"),
        F.first("store_id").over(w2).alias("first_store_id"),
    )
)
